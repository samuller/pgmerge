#!/usr/bin/env python3
"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import re
import sys
import click
import logging
import sqlalchemy
from .utils import *
from .db_config import *
from appdirs import user_log_dir
from logging.handlers import RotatingFileHandler
from . import db_graph, db_import, db_export, db_inspect, __version__

APP_NAME = "pgmerge"
LOG_FILE = os.path.join(user_log_dir(APP_NAME, appauthor=False), "out.log")

log = logging.getLogger()


def setup_logging(verbose=False):
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    max_total_size = 1024 * 1024
    file_count = 2
    file_handler = RotatingFileHandler(LOG_FILE, mode='a', maxBytes=max_total_size / file_count,
                                       backupCount=file_count - 1, encoding=None, delay=0)
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)-10.10s %(threadName)-12.12s %(levelname)-8.8s  %(message)s"))
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        NoExceptionFormatter("%(levelname)s: %(message)s"))
    stream_handler.setLevel(logging.WARN)
    # Get the root logger to setup logging for all other modules
    log.addHandler(file_handler)
    log.addHandler(stream_handler)
    # Set the root level to lowest detail otherwise it's never passed on to handlers or other loggers
    log.setLevel(logging.DEBUG)
    # Example of separately controlling log level of imported modules
    # logging.getLogger(db_export.__name__).setLevel(logging.WARN)
    if verbose:
        file_handler.setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)


def find_and_warn_about_cycles(table_graph, dest_tables):
    def print_message(msg):
        print(msg)
        print()
        print("Import will require the --disable-foreign-keys option.")

    simple_cycles = db_graph.get_cycles(table_graph)

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


def import_all_new(connection, inspector, schema, import_files, dest_tables, file_format=None,
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
    total_stats = {'skip': 0, 'insert': 0, 'update': 0, 'total': 0}
    error_tables = list(unknown_tables)

    if suspend_foreign_keys:
        db_import.disable_foreign_key_constraints(cursor)
    elif find_and_warn_about_cycles(table_graph, dest_tables):
        log.warning("Import cancelled due to detected cycles")
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


def get_import_files_and_tables(directory, tables):
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

    return import_files, dest_tables


def validate_schema(inspector, schema):
    if schema is None:
        schema = inspector.default_schema_name
    if schema not in inspector.get_schema_names():
        print("Schema not found: '{}'".format(schema))
        sys.exit()
    return schema


def validate_tables(inspector, schema, tables):
    if len(tables) == 0:
        return None
    all_tables = set(inspector.get_table_names(schema))
    unknown_tables = set(tables) - all_tables
    if len(unknown_tables) > 0:
        print("Tables not found in database:")
        print("\t" + "\n\t".join(unknown_tables))
        sys.exit()
    return tables


def check_table_params(ctx, param, value):
    """
    Callback function to check table command-line arguments.
    """
    assert param.name == 'tables'
    other_flag = 'include_dependent_tables'
    if len(value) == 0 and other_flag in ctx.params and ctx.params[other_flag] is True:
        raise click.UsageError(
            "Illegal usage: '{}' option is only valid if '{}' arguments have been specified."
            .format(other_flag, param.name))
    return value


# Shared command line options for connecting to a database
db_connect_options = [
    click.option('--dbname', '-d', help='Database name to connect to.', required=True),
    click.option('--host', '-h', help='Database server host or socket directory.',
                 default='localhost', show_default=True),
    click.option('--port', '-p', help='Database server port.', default='5432', show_default=True),
    click.option('--username', '-U', help='Database user name.', default='postgres', show_default=True),
    click.option('--schema', '-s', default="public", help='Database schema to use.',
                 show_default=True),
    click.option('--password', '-W', hide_input=True, prompt=False, default=None,
                 help='Database password (default is to prompt for password or read config).')
]

# Shared command line arguments for importing/exporting tables to a directory
dir_tables_arguments = [
    click.option('--config', '-c', type=click.Path(exists=True, dir_okay=False),
                 help='Config file for customizing data import/export.'),
    click.option(
        '--include-dependent-tables', '-i', is_flag=True,
        help='When selecting specific tables, also include ' +
             'all tables that depend on those tables due to foreign key constraints.'),
    click.argument('directory', nargs=1, type=click.Path(exists=True, file_okay=False)),
    click.argument('tables', default=None, nargs=-1, callback=check_table_params)
]


@click.group(context_settings=dict(max_content_width=120))
@click.option('--verbose', '-v', is_flag=True, help='Give more verbose output.')
@click.version_option(version=__version__, message="%(prog)s, version %(version)s\nSimon Muller <samullers@gmail.com>")
def main(verbose):
    """
    Merges data in CSV files into a Postgresql database.
    """
    setup_logging(verbose)


@main.command()
@decorate(db_connect_options)
@decorate(dir_tables_arguments)
def export(dbname, host, port, username, password, schema,
           config, include_dependent_tables,
           directory, tables):
    """
    Export each table to a CSV file.

    If one or more tables are specified then only they will be used, otherwise all tables found will be selected. They
    will all be exported into the given directory.
    """
    engine = None
    try:
        password = retrieve_password(APP_NAME, dbname, host, port, username, password)
        db_url = generate_url(dbname, host, port, username, password)
        engine = sqlalchemy.create_engine(db_url)
        inspector = sqlalchemy.inspect(engine)
        schema = validate_schema(inspector, schema)
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = validate_tables(inspector, schema, tables)
        if include_dependent_tables:
            tables = db_graph.get_all_dependent_tables(table_graph, tables)
        if tables is None:
            tables = sorted(inspector.get_table_names(schema))

        colums_per_table = None
        if config is not None:
            table_config = load_config_for_tables(config)
            config_valid, reason = validate_table_config_with_schema(inspector, schema, table_config)
            if not config_valid:
                print(reason)
                sys.exit()
            colums_per_table = {key: table_config[key]['columns']
                                for key in table_config.keys() if 'columns' in table_config[key]}

        find_and_warn_about_cycles(table_graph, tables)

        run_in_session(engine, lambda conn:
                       db_export.export_columns(conn, schema, directory, tables, columns=colums_per_table))
        print("Exported {} tables".format(len(tables)))
    except Exception as e:
        logging.exception(e)
    finally:
        if engine is not None:
            engine.dispose()


@main.command(name="import")
@decorate(db_connect_options)
@click.option('--disable-foreign-keys', '-f', is_flag=True,
              help='Disable foreign key constraint checking during import (necessary if you have cycles, but ' +
                   'requires superuser rights).')
@decorate(dir_tables_arguments)
def upsert(dbname, host, port, username, password, schema,
           config, include_dependent_tables, disable_foreign_keys,
           directory, tables):
    """
    Import/merge each CSV file into a table.

    All CSV files need the same name as their matching table and have to be located in the given directory.
    If one or more tables are specified then only they will be used, otherwise all tables
    found will be selected.
    """
    engine = None
    try:
        password = retrieve_password(APP_NAME, dbname, host, port, username, password)
        db_url = generate_url(dbname, host, port, username, password)
        engine = sqlalchemy.create_engine(db_url)
        inspector = sqlalchemy.inspect(engine)
        schema = validate_schema(inspector, schema)
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = validate_tables(inspector, schema, tables)
        if include_dependent_tables:
            tables = db_graph.get_all_dependent_tables(table_graph, tables)

        import_files, dest_tables = get_import_files_and_tables(directory, tables)
        run_in_session(engine, lambda conn:
                       import_all_new(conn, inspector, schema, import_files, dest_tables,
                                      suspend_foreign_keys=disable_foreign_keys))
    except Exception as e:
        logging.exception(e)
    finally:
        if engine is not None:
            engine.dispose()


@main.command(context_settings=dict(max_content_width=120))
@click.option('--engine', '-e', help="Type of database engine.", default='postgresql', show_default=True)
@decorate(db_connect_options)
@click.option('--warnings', '-w', is_flag=True, help="Output any issues detected in database schema.")
@click.option('--list-tables', '-t', is_flag=True, help="Output all tables found in the given schema.")
@click.option('--table-details', '-td', is_flag=True,
              help="Output all tables along with column and foreign key information.")
@click.option('--cycles', '-c', is_flag=True, help="Find and list cycles in foreign-key dependency graph.")
@click.option('--insert-order', '-i', is_flag=True,
              help="Output the insertion order of tables based on the foreign-key dependency graph. " +
                   "This can be used by importer scripts if there are no circular dependency issues.")
@click.option('--partition', '-pt', is_flag=True,
              help="Partition and list sub-graphs of foreign-key dependency graph.")
@click.option('--export-graph', '-x', is_flag=True,
              help="Output dot format description of foreign-key dependency graph." +
                   " To use graphviz to generate a PDF from this format, pipe the output to:" +
                   " dot -Tpdf > graph.pdf")
@click.option('--transferable', '-tf', is_flag=True, help="Output info related to table transfers.")
def inspect(engine, dbname, host, port, username, password, schema,
            warnings, list_tables, table_details, partition,
            cycles, insert_order, export_graph, transferable):
    """
    Inspect database schema in various ways.

    Defaults to PostgreSQL but should support multiple database engines thanks to SQLAlchemy (see:
    http://docs.sqlalchemy.org/en/latest/dialects/).
    """
    _engine = None
    try:
        password = retrieve_password(APP_NAME, dbname, host, port, username, password)
        db_url = generate_url(dbname, host, port, username, password, type=engine)
        _engine = sqlalchemy.create_engine(db_url)
        db_inspect.main(_engine, schema,
                        warnings, list_tables, table_details, partition,
                        cycles, insert_order, export_graph, transferable)
    except Exception as e:
        logging.exception(e)
    finally:
        if _engine is not None:
            _engine.dispose()


if __name__ == "__main__":
    main()
