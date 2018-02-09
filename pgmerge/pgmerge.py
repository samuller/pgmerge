#!/usr/bin/env python3
import os
import re
import click
import logging
import getpass
from . import db_graph
from .db_config import *
from sqlalchemy import create_engine, inspect
from logging.handlers import RotatingFileHandler
from appdirs import user_log_dir

APP_NAME = "pgmerge"
LOG_FILE = os.path.join(user_log_dir(APP_NAME, appauthor=False), "out.log")


def setup_logging():
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    max_total_size = 1024*1024
    file_count = 2
    file_handler = RotatingFileHandler(LOG_FILE, mode='a', maxBytes=max_total_size/file_count,
                                       backupCount=file_count - 1, encoding=None, delay=0)
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)-10.10s %(threadName)-12.12s %(levelname)-8.8s  %(message)s"))
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(levelname)s: %(message)s"))
    stream_handler.setLevel(logging.WARN)

    logging.basicConfig(handlers=[file_handler, stream_handler])


def export_all(connection, inspector, schema, output_dir, tables=None, file_format="CSV HEADER"):
    cursor = connection.cursor()
    if tables is None:
        tables = sorted(inspector.get_table_names(schema))

    table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
    find_and_warn_about_cycles(table_graph, tables)

    for table in tables:
        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY %s TO STDOUT WITH %s' % (table, file_format)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()


def get_unique_columns(inspector, table, schema):
    """
    If the combination of primary key and unique constraints is used to identify a row, then you'll miss rows where
    values in separate unique constraints have been swapped. This means that extra INSERTS or missed UPDATES could
    happen if these columns are collectively used as an identifier.
    """
    pks = inspector.get_primary_keys(table, schema)
    unique_constraints = inspector.get_unique_constraints(table, schema)
    unique = [col for constraint in unique_constraints for col in constraint['column_names']]
    return pks + unique


def sql_delete_identical_rows_between_tables(delete_table_name, reference_table_name, all_column_names):
    # "IS NOT DISTINCT FROM" handles NULLS better (even composite type columns), but is not indexed
    # where_clause = " AND ".join(["%s.%s IS NOT DISTINCT FROM %s.%s" % (table, col, temp_table_name, col)
    #                               for col in all_columns])
    where_clause = " AND ".join(
        ["(%s.%s = %s.%s OR (%s.%s IS NULL AND %s.%s IS NULL))"
         % (reference_table_name, col, delete_table_name, col, reference_table_name, col, delete_table_name, col)
         for col in all_column_names])
    delete_sql = "DELETE FROM %s USING %s WHERE %s;" % \
                 (delete_table_name, reference_table_name, where_clause)
    return delete_sql


def sql_insert_rows_not_in_table(insert_table_name, reference_table_name, id_column_names):
    insert_table_cols = ",".join(["%s.%s" % (insert_table_name, col) for col in id_column_names])
    reference_table_cols = ",".join(["%s.%s" % (reference_table_name, col) for col in id_column_names])

    select_sql = "SELECT %s.* FROM %s LEFT JOIN %s ON (%s) = (%s) WHERE (%s) is NULL" %\
                 (reference_table_name, reference_table_name, insert_table_name,
                  insert_table_cols, reference_table_cols, insert_table_cols)

    insert_sql = "INSERT INTO %s (%s) RETURNING NULL;" % (insert_table_name, select_sql)
    return insert_sql


def sql_update_rows_between_tables(update_table_name, reference_table_name, id_column_names, all_column_names):
    # UPDATE table_b SET column1 = a.column1, column2 = a.column2, column3 = a.column3
    # FROM table_a WHERE table_a.id = table_b.id AND table_b.id in (1, 2, 3)
    set_columns = ",".join(["%s = %s.%s" % (col, reference_table_name, col)
                            for col in all_column_names])
    where_clause = " AND ".join(["%s.%s = %s.%s" % (update_table_name, col, reference_table_name, col)
                                 for col in id_column_names])
    update_sql = "UPDATE %s SET %s FROM %s WHERE %s" % \
                 (update_table_name, set_columns, reference_table_name, where_clause)
    return update_sql


def import_new(inspector, cursor, schema, dest_table, input_file, file_format="CSV HEADER"):
    """
    Postgresql 9.5+ includes merge/upsert with INSERT ... ON CONFLICT, but it requires columns to have unique
    constraints (or even a partial unique index). We might use it once we're sure that it covers all our use cases.
    """
    id_columns = get_unique_columns(inspector, dest_table, schema)
    if len(id_columns) == 0:
        return None

    all_columns = [col['name'] for col in inspector.get_columns(dest_table, schema)]
    stats = {'skip': 0, 'insert': 0, 'update': 0, 'total': 0}

    temp_table_name = "_tmp_%s" % (dest_table,)
    input_file = open(input_file, 'r')
    # Create temporary table with same columns and types as target table
    create_sql = "CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0;" % (temp_table_name, dest_table)
    cursor.execute(create_sql)
    # Import data into temporary table
    copy_sql = 'COPY %s FROM STDOUT WITH %s' % (temp_table_name, file_format)
    cursor.copy_expert(copy_sql, input_file)
    stats['total'] = cursor.rowcount

    # Delete rows in temp table that are already identical to those in destination table
    cursor.execute(sql_delete_identical_rows_between_tables(temp_table_name, dest_table, all_columns))
    stats['skip'] = cursor.rowcount

    # Insert rows from temp table that are not in destination table (according to id columns)
    cursor.execute(sql_insert_rows_not_in_table(dest_table, temp_table_name, id_columns))
    stats['insert'] = cursor.rowcount
    # Delete rows that were just inserted
    cursor.execute(sql_delete_identical_rows_between_tables(temp_table_name, dest_table, all_columns))

    # Update rows whose id columns match in destination table
    cursor.execute(sql_update_rows_between_tables(dest_table, temp_table_name, id_columns, all_columns))
    stats['update'] = cursor.rowcount

    drop_sql = "DROP TABLE %s" % (temp_table_name,)
    cursor.execute(drop_sql)

    # VACUUM is useful for each table that had major updates/import, but it has to run outside a transaction
    # and requires connection to be in autocommit mode
    # cursor.execute("VACUUM ANALYZE %s" % (dest_table,))

    return stats


def disable_foreign_key_constraints(cursor):
    """
    There are different possible approaches for disabling foreign keys. The following are some options that
    disable and re-enable foreign keys globally [1]:
        SET session_replication_role = REPLICA; -- [2]
        SET session_replication_role = DEFAULT;
    or
        SET CONSTRAINTS ALL DEFERRED;
        SET CONSTRAINTS ALL IMMEDIATE;

    Options for disabling and re-enabling foreign keys per table are [3]:
        ALTER TABLE table_name DISABLE TRIGGER ALL;
        ALTER TABLE table_name ENABLE TRIGGER ALL;
    or
        ALTER TABLE table_name ALTER CONSTRAINT table_fkey DEFERRABLE; -- [4]
        SET CONSTRAINTS table_fkey DEFERRED;
    or
        ALTER TABLE table_name DROP CONSTRAINT table_fkey;
        ALTER TABLE table_name ADD CONSTRAINT table_fkey FOREIGN KEY (column_name)
            REFERENCES other_table_name (other_column_name) ON DELETE RESTRICT;
    The second and third options might need extra code to leave constraints in the same state as before (e.g.
    NOT DEFERRABLE or ON UPDATE CASCADE).

    Some of these options work on most constraints, but not foreign keys. Most cases where foreign keys can
    be disabled, it seems to require superuser rights.

    [1][https://stackoverflow.com/questions/3942258/how-do-i-temporarily-disable-triggers-in-postgresql]
    [2][https://www.postgresql.org/docs/current/static/runtime-config-client.html]
    [3][https://stackoverflow.com/questions/38112379/disable-postgresql-foreign-key-checks-for-migrations]
    [4][https://www.postgresql.org/docs/current/static/sql-altertable.html]
    """
    sql = "SET session_replication_role = REPLICA;"
    cursor.execute(sql)


def enable_foreign_key_constraints(cursor):
    sql = "SET session_replication_role = DEFAULT;"
    cursor.execute(sql)


def find_and_warn_about_cycles(table_graph, dest_tables):
    def print_message(msg):
        print(msg)
        print()
        print("See --help regarding the --disable-foreign-keys option.")

    simple_cycles = db_graph.get_simple_cycles(table_graph)

    relevant_cycles = [cycle for cycle in simple_cycles if len(cycle) > 1 if set(cycle).issubset(set(dest_tables))]
    if len(relevant_cycles) > 0:
        print_message("Table dependencies contain cycles that could prevent import:\n\t%s" % (relevant_cycles,))
        return True

    self_references = [table for cycle in simple_cycles if len(cycle) == 1 for table in cycle]
    relevant_tables = [table for table in self_references if table in dest_tables]
    if len(relevant_tables) > 0:
        print_message("Self-referencing tables found that could prevent import:\n\n\t%s" % (relevant_tables,))
        return True

    return False


def get_and_warn_about_any_unknown_tables(import_files, dest_tables, schema_tables):
    unknown_tables = set(dest_tables).difference(set(schema_tables))
    if len(unknown_tables) > 0:
        print("Skipping files for unknown tables:")
        for table in unknown_tables:
            idx = dest_tables.index(table)
            print("\t%s: %s" % (table, import_files[idx]))
            del dest_tables[idx]
            del import_files[idx]
        print()
    return unknown_tables


def import_all_new(connection, inspector, schema, import_files, dest_tables, file_format="CSV HEADER",
                   suspend_foreign_keys=False):
    """
    Imports files that introduce new or updated rows. These files have the exact structure
    of the final desired table except that they might be missing rows.
    """
    assert len(import_files) == len(dest_tables), "Files without matching tables"
    # Use copy of lists since they might be altered and are passed by reference
    import_files = list(import_files)
    dest_tables = list(dest_tables)

    cursor = connection.cursor()

    tables = sorted(inspector.get_table_names(schema))
    unknown_tables = get_and_warn_about_any_unknown_tables(import_files, dest_tables, tables)

    table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
    # Sort by dependency requirements
    insertion_order = db_graph.get_insertion_order(table_graph)
    import_pairs = list(zip(import_files, dest_tables))
    import_pairs.sort(key=lambda pair: insertion_order.index(pair[1]))
    # Stats
    total_stats = {'skip': 0, 'insert': 0, 'update': 0}
    error_tables = list(unknown_tables)

    if suspend_foreign_keys:
        disable_foreign_key_constraints(cursor)
    elif find_and_warn_about_cycles(table_graph, dest_tables):
        return

    for file, table in import_pairs:
        print("%s:" % (table,))
        stats = import_new(inspector, cursor, schema, table, file, file_format)
        if stats is None:
            print("\tSkipping table as it has no primary key or unique columns!")
            error_tables.append(table)
            continue

        stat_output = "\t skip: {0:<10} insert: {1:<10} update: {2}".format(
            stats['skip'], stats['insert'], stats['update'])
        if stats['insert'] > 0 or stats['update']:
            click.secho(stat_output, fg='green')
        else:
            print(stat_output)
        total_stats = {k: total_stats.get(k, 0) + stats.get(k, 0) for k in set(total_stats) | set(stats)}

    if suspend_foreign_keys:
        enable_foreign_key_constraints(cursor)

    print()
    print("Total results:\n\t skip: %s \n\t insert: %s \n\t update: %s \n\t total: %s" %
          (total_stats['skip'], total_stats['insert'], total_stats['update'], total_stats['total']))
    if len(error_tables) > 0:
        print("\n%s tables skipped due to errors:" % (len(error_tables)))
        print("\t" + "\n\t".join(error_tables))
    print("\n%s tables imported successfully" % (len(dest_tables) - len(error_tables),))

    # Transaction is committed
    connection.commit()


def run_in_session(engine, func):
    conn = engine.raw_connection()
    try:
        func(conn)
    finally:
        conn.close()


@click.command(context_settings=dict(max_content_width=120))
@click.option('--dbname', '-d', help='database name to connect to', required=True)
@click.option('--host', '-h', help='database server host or socket directory', default='localhost', show_default=True)
@click.option('--port', '-p', help='database server port', default='5432', show_default=True)
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use',  show_default=True)
@click.option('--password', '-W', hide_input=True, prompt=False, default=None,
              help='database password (default is to prompt for password or read config)')
# @click.option('--config', '-c', help='config file')
@click.option('--include-dependent-tables', '-i', is_flag=True, help='when selecting specific tables, also include ' +
              'all tables that depend on those tables due to foreign key constraints')
@click.option('--disable-foreign-keys', '-f', is_flag=True,
              help='disable foreign key constraint checking during import (necessary if you have cycles, but ' +
                   'requires superuser rights)')
@click.option('--export', '-e', is_flag=True, help='instead of import/merge, export all tables to directory')
@click.argument('directory', default='tmp', nargs=1)
@click.argument('tables', default=None, nargs=-1)
@click.version_option(version='0.9.0')
def main(dbname, host, port, username, password, schema,
         export, directory, tables, disable_foreign_keys, include_dependent_tables):
    """
    Merges data in CSV files (from the given directory, default: 'tmp') into a Postgresql database.
    If one or more tables are specified then only they will be used, otherwise all tables found will be selected.
    """
    setup_logging()

    config_db_user = {'host': host, 'port': port, 'username': username, 'password': password}
    config_db = load_config_for_db(APP_NAME, dbname, config_db_user)
    if config_db is None:
        return
    if config_db['password'] is None:
        config_db['password'] = getpass.getpass()

    url = "postgresql://{username}:{password}@{host}:{port}/{dbname}".format(**config_db, dbname=dbname)
    engine = create_engine(url)
    inspector = inspect(engine)
    if schema is None:
        schema = inspector.default_schema_name

    if len(tables) == 0:
        tables = None

    if include_dependent_tables:
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = db_graph.get_all_dependent_tables(table_graph, tables)

    if export:
        run_in_session(engine, lambda conn:
            export_all(conn, inspector, schema, directory, tables)
        )
    else:
        # Determine tables based no files in directory
        all_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        import_files = [f for f in all_files if re.match(r".*\.csv", f)]
        dest_tables = [f[:-4] for f in import_files]
        if len(tables) != 0:
            # Look for files based on given tables
            import_files = ["%s.csv" % (table,) for table in tables]
            dest_tables = tables
            unknown_files = set(import_files).difference(set(all_files))
            if len(unknown_files) > 0:
                print("No files found for the following tables:")
                for file in unknown_files:
                    print("\t", file)
                return
        import_files = [os.path.join(directory, f) for f in import_files]

        run_in_session(engine, lambda conn:
            import_all_new(conn, inspector, schema, import_files, dest_tables,
                           suspend_foreign_keys=disable_foreign_keys)
        )


if __name__ == "__main__":
    main()
