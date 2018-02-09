#!/usr/bin/env python3
import os
import re
import click
import logging
import getpass
from .db_config import *
from appdirs import user_log_dir
from .utils import NoExceptionFormatter
from . import db_graph, db_import, db_export
from sqlalchemy import create_engine, inspect
from logging.handlers import RotatingFileHandler

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
        NoExceptionFormatter("%(levelname)s: %(message)s"))
    stream_handler.setLevel(logging.WARN)

    logging.basicConfig(handlers=[file_handler, stream_handler])


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


def import_all_new(connection, inspector, schema, import_files, dest_tables, file_format="FORMAT CSV, HEADER",
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
        db_import.disable_foreign_key_constraints(cursor)
    elif find_and_warn_about_cycles(table_graph, dest_tables):
        return

    for file, table in import_pairs:
        print("%s:" % (table,))
        stats = db_import.pg_upsert(inspector, cursor, schema, table, file, file_format)
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
        db_import.enable_foreign_key_constraints(cursor)

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


def combine_db_configs_to_get_url(dbname, host, port, username, password):
    """
    Combine command-line parameters with default config and request password if not yet provided.

    Command-line parameters take priority over defaults in config file.
    """
    config_db_user = {'host': host, 'port': port, 'username': username, 'password': password}
    config_db = load_config_for_db(APP_NAME, dbname, config_db_user)
    if config_db is None:
        return
    if config_db['password'] is None:
        config_db['password'] = getpass.getpass()

    url = "postgresql://{username}:{password}@{host}:{port}/{dbname}".format(**config_db, dbname=dbname)
    return url


def process_args_and_run(dbname, host, port, username, password, schema,
                         export, directory, tables, disable_foreign_keys, include_dependent_tables):
    url = combine_db_configs_to_get_url(dbname, host, port, username, password)
    engine = create_engine(url)
    inspector = inspect(engine)
    if schema is None:
        schema = inspector.default_schema_name

    if not os.path.exists(directory):
        print("Directory not found: '{}'".format(directory))
        return

    if len(tables) == 0:
        tables = None
    else:
        # Check tables exist in database
        all_tables = set(inspector.get_table_names(schema))
        unknown_tables = set(tables) - all_tables
        if len(unknown_tables) > 0:
            print("Unknown tables (not found in database):")
            print("\t" + "\n\t".join(unknown_tables))
            return


    if include_dependent_tables and tables is None:
        print('Option to specifically include dependent tables has been ignored as all tables will be imported.')
        print()
    elif include_dependent_tables:
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = db_graph.get_all_dependent_tables(table_graph, tables)

    if export:
        if tables is None:
            tables = sorted(inspector.get_table_names(schema))
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        find_and_warn_about_cycles(table_graph, tables)

        run_in_session(engine, lambda conn:
            db_export.export_all(conn, inspector, schema, directory, tables)
        )
    else:
        # Determine tables based on files in directory
        all_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        import_files = [f for f in all_files if re.match(r".*\.csv", f)]
        dest_tables = [f[:-4] for f in import_files]
        if tables is not None and len(tables) != 0:
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
    try:
        process_args_and_run(dbname, host, port, username, password, schema,
                             export, directory, tables, disable_foreign_keys, include_dependent_tables)
    except Exception as e:
        logging.exception(e)


if __name__ == "__main__":
    main()
