#!/usr/bin/env python3
import os
import re
import click
from sqlalchemy import create_engine, inspect

found_config = True
try:
    import config as cfg
except ImportError:
    found_config = False


def export_all(engine, inspector, schema, output_dir, file_format="CSV HEADER"):
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()

        tables = sorted(inspector.get_table_names(schema))
        for table in tables:
            output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
            copy_sql = 'COPY %s TO STDOUT WITH %s' % (table, file_format)
            cursor.copy_expert(copy_sql, output_file)

        conn.commit()
    finally:
        conn.close()


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
    stats = {'skip': 0, 'insert': 0, 'update': 0}

    temp_table_name = "_tmp_%s" % (dest_table,)
    input_file = open(input_file, 'r')
    # Create temporary table with same columns and types as target table
    create_sql = "CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0;" % (temp_table_name, dest_table)
    cursor.execute(create_sql)
    # Import data into temporary table
    copy_sql = 'COPY %s FROM STDOUT WITH %s' % (temp_table_name, file_format)
    cursor.copy_expert(copy_sql, input_file)

    # Insert rows from temp table that are not in destination table (according to id columns)
    cursor.execute(sql_insert_rows_not_in_table(dest_table, temp_table_name, id_columns))
    stats['insert'] = cursor.rowcount

    # Delete rows in temp table that are already identical to those in destination table
    cursor.execute(sql_delete_identical_rows_between_tables(temp_table_name, dest_table, all_columns))
    stats['skip'] = cursor.rowcount

    # Update rows whose id columns match in destination table
    cursor.execute(sql_update_rows_between_tables(dest_table, temp_table_name, id_columns, all_columns))
    stats['update'] = cursor.rowcount

    drop_sql = "DROP TABLE %s" % (temp_table_name,)
    cursor.execute(drop_sql)

    return stats


def import_all_new(engine, inspector, schema, import_files, dest_tables, file_format="CSV HEADER"):
    """
    Imports files that introduce new or updated rows. These files have the exact structure
    of the final desired table except that they might be missing rows.
    """
    assert len(import_files) == len(dest_tables), "Files without matching tables"

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()

        tables = sorted(inspector.get_table_names(schema))
        total_stats = {'skip': 0, 'insert': 0, 'update': 0}
        error_tables = []

        for file, table in zip(import_files, dest_tables):
            if table not in tables:
                print("%s:\n\tSkipping unknown table for '%s'!" % (table, file))
                error_tables.append(table)
                continue

            stats = import_new(inspector, cursor, schema, table, file, file_format)

            if stats is None:
                print("%s:\n\tSkipping table as it has no primary key or unique columns!" % (table,))
                error_tables.append(table)
                continue

            print("%s:\n\t skip: %s \t insert: %s \t update: %s" %
                  (table, stats['skip'], stats['insert'], stats['update']))
            total_stats = {k: total_stats.get(k, 0) + stats.get(k, 0) for k in set(total_stats) | set(stats)}

        print()
        print("Total results:\n\t skip: %s \n\t insert: %s \n\t update: %s" %
              (total_stats['skip'], total_stats['insert'], total_stats['update']))
        print("\n%s tables skipped due to errors:" % (len(error_tables)))
        print("\t" + "\n\t".join(error_tables))
        print("\n%s tables imported successfully" % (len(dest_tables) - len(error_tables),))

        conn.commit()
    finally:
        conn.close()


@click.command(context_settings=dict(max_content_width=120))
@click.option('--dbname', '-d', help='database name to connect to', required=True)
@click.option('--host', '-h', help='database server host or socket directory (default: localhost)', default='localhost')
@click.option('--port', '-p', help='database server port (default: 5432)', default='5432')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use (default: public)')
@click.option('--password', '-W', hide_input=True, prompt=not found_config,
              default=cfg.DB_PASSWORD if found_config else None,
              help='database password (default is to prompt for password or read config)')
@click.option('--export', '-e', is_flag=True, help='export all tables to directory')
@click.option('--config', '-c', help='config file')
@click.argument('directory', default='tmp')
@click.version_option(version='0.0.1')
def main(dbname, host, port, username, password, schema,
         config, export, directory):

    url = "postgresql://%s:%s@%s:%s/%s" % (username, password, host, port, dbname)
    engine = create_engine(url)
    inspector = inspect(engine)
    if schema is None:
        schema = inspector.default_schema_name
        # print(inspector.get_schema_names())

    if export:
        export_all(engine, inspector, schema, directory)
    else:
        all_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        import_files = [f for f in all_files if re.match(".*\.csv", f)]
        dest_tables = [f[:-4] for f in import_files]
        import_files = [os.path.join(directory, f) for f in all_files if re.match(".*\.csv", f)]
        import_all_new(engine, inspector, schema, import_files, dest_tables)


if __name__ == "__main__":
    main()
