#!/usr/bin/env python3
"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2024 Simon Muller (samullers@gmail.com)
"""
import os
import re
import sys
import errno
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Optional, List, Dict, Set, Tuple, Union, Callable, cast

import typer
import click
import sqlalchemy
from platformdirs import user_log_dir

from .utils import decorate, NoExceptionFormatter, only_file_stem
from .db_config import (
    load_config_for_tables,
    validate_table_configs_with_schema,
    retrieve_password,
    generate_url,
    convert_to_config_per_subset,
    ConfigInvalidException,
    TablesConfig,
)
from . import db_graph, db_import, db_export, db_inspect, __version__

APP_NAME = "pgmerge"
LOG_FILE = os.path.join(user_log_dir(APP_NAME, appauthor=False), "out.log")

EXIT_CODE_ARGS = 2
# Use exit code 3 for exceptions since click already returns 1 and 2
# (1 for aborts and 2 invalid arguments)
EXIT_CODE_EXC = 3
# Invalid data in either files or database (e.g. file data and tables don't match up)
EXIT_CODE_INVALID_DATA = 4

log = logging.getLogger()


app = typer.Typer(
    help="Merge data in CSV files into a Postgresql database.",
    context_settings=dict(max_content_width=120),
    add_completion=False,
)


def setup_logging(verbose: bool = False) -> None:  # pragma: no cover
    """Set up logging for the whole app."""
    log_dir = os.path.dirname(LOG_FILE)
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        max_total_size = 1024 * 1024
        file_count = 2
        file_handler = RotatingFileHandler(
            LOG_FILE,
            mode="a",
            maxBytes=max_total_size // file_count,
            backupCount=file_count - 1,
            encoding=None,
            delay=False,
        )
    except OSError as err:
        if err.errno == errno.EACCES:
            print("WARN: No permissions to create logging directory or file: " + LOG_FILE)
            return
        raise err

    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(name)-10.10s %(threadName)-12.12s %(levelname)-8.8s  %(message)s")
    )
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(NoExceptionFormatter("%(levelname)s: %(message)s"))
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


def find_and_warn_about_cycles(table_graph: Any, dest_tables: List[str]) -> bool:
    """Check and warn if the parts of database schema being used have foreign keys containing cycles."""

    def print_message(msg: str) -> None:
        print(msg)
        print("Import might require the --disable-foreign-keys option.")
        print()

    simple_cycles = db_graph.get_cycles(table_graph)

    relevant_cycles = [cycle for cycle in simple_cycles if len(cycle) > 1 if set(cycle).issubset(set(dest_tables))]
    if len(relevant_cycles) > 0:
        print_message("Table dependencies contain cycles that could prevent import:\n\t{}".format(relevant_cycles))
        return True

    self_references = [table for cycle in simple_cycles if len(cycle) == 1 for table in cycle]
    relevant_tables = [table for table in self_references if table in dest_tables]
    if len(relevant_tables) > 0:
        print_message(
            "Self-referencing tables found that could prevent import: {}".format(", ".join(sorted(relevant_tables)))
        )
        return True

    return False


def get_and_warn_about_any_unknown_tables(
    import_files: List[str], dest_tables: List[str], schema_tables: List[str]
) -> Tuple[List[str], Set[str]]:
    """Compare tables expected for import with actual in schema and warn about inconsistencies."""
    unknown_tables = set(dest_tables).difference(set(schema_tables))
    skipped_files = []
    if len(unknown_tables) > 0:
        print("Skipping files for unknown tables:")
        for table in unknown_tables:
            idx = dest_tables.index(table)
            print("\t%s: %s" % (table, import_files[idx]))
            skipped_files.append(import_files[idx])
            del dest_tables[idx]
            del import_files[idx]
        print()
    # TODO: have common data structure for file/table pairs
    return skipped_files, unknown_tables


def _get_table_name_with_file(file_name: str, table_name: str) -> str:
    file_stem = only_file_stem(file_name)
    if file_stem == table_name:
        return table_name
    return "{} [{}]".format(table_name, file_stem)


def import_all_new(
    connection: Any,
    inspector: Any,
    schema: str,
    import_files: List[str],
    dest_tables: List[str],
    config_per_table: Optional[TablesConfig] = None,
    file_format: Optional[str] = None,
    suspend_foreign_keys: bool = False,
    fail_on_warning: bool = True,
) -> None:
    """
    Import files that introduce new or updated rows.

    These files have the exact structure of the final desired table except that they might be missing rows.
    """
    assert len(import_files) == len(dest_tables), "Files without matching tables"
    if config_per_table is None:
        config_per_table = {}
    # Use copy of lists since they might be altered and are passed by reference
    import_files = list(import_files)
    dest_tables = list(dest_tables)

    # This should be the default (see: https://www.psycopg.org/docs/connection.html#connection.autocommit)
    # but it helps make it clear that we're follow the PostgreSQL recommendation:
    # https://www.postgresql.org/docs/current/populate.html#DISABLE-AUTOCOMMIT
    connection.autocommit = False

    if connection.encoding != "UTF8":
        print("WARNING: Setting database connection encoding to UTF8 instead of '{}'".format(connection.encoding))
        connection.set_client_encoding("UTF8")

    cursor = connection.cursor()

    # Count destination tables before invalid ones are removed
    expected_dest_tables_count = len(set(dest_tables))
    expected_import_files_count = len(import_files)
    tables = sorted(inspector.get_table_names(schema))
    skipped_files, unknown_tables = get_and_warn_about_any_unknown_tables(import_files, dest_tables, tables)
    assert len(import_files) == len(dest_tables), "Files without matching tables after skips"

    table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
    # Sort by dependency requirements
    insertion_order = db_graph.get_insertion_order(table_graph)
    import_pairs = list(zip(import_files, dest_tables))
    import_pairs.sort(key=lambda pair: insertion_order.index(pair[1]))
    # Stats
    total_stats = {"skip": 0, "insert": 0, "update": 0, "total": 0}
    error_tables = list(unknown_tables)

    if suspend_foreign_keys:
        db_import.disable_foreign_key_constraints(cursor)
    elif find_and_warn_about_cycles(table_graph, dest_tables) and fail_on_warning:
        log.warning("Import cancelled due to detected cycles")
        return

    config_per_subset = convert_to_config_per_subset(config_per_table)
    for file, table in import_pairs:
        print("{}:".format(_get_table_name_with_file(file, table)))

        subset_name = only_file_stem(file)
        file_config = config_per_subset.get(subset_name, None)
        try:
            stats = db_import.pg_upsert(
                inspector,
                cursor,
                schema,
                table,
                file,
                file_format,
                file_config=file_config,
                config_per_table=config_per_table,
            )
        except db_import.UnsupportedSchemaException as exc:
            print("\tSkipping table with unsupported schema: {}".format(exc))
            error_tables.append(table)
            skipped_files.append(file)
            continue

        stat_output = "\t skip: {0:<10} insert: {1:<10} update: {2}".format(
            stats["skip"], stats["insert"], stats["update"]
        )
        if stats["insert"] > 0 or stats["update"]:
            click.secho(stat_output, fg="green")
        else:
            print(stat_output)
        new_stats = cast(Dict[str, int], stats)  # type: ignore
        total_stats = {k: total_stats.get(k, 0) + new_stats.get(k, 0) for k in set(total_stats) | set(new_stats)}

    if suspend_foreign_keys:
        db_import.enable_foreign_key_constraints(cursor)

    print()
    print(
        "Total results:\n\t skip: %s \n\t insert: %s \n\t update: %s \n\t total: %s"
        % (total_stats["skip"], total_stats["insert"], total_stats["update"], total_stats["total"])
    )
    if len(error_tables) > 0:
        print("\n%s tables skipped due to errors:" % (len(error_tables)))
        print("\t" + "\n\t".join(error_tables))
    success_tables = expected_dest_tables_count - len(error_tables)
    success_files = expected_import_files_count - len(skipped_files)
    print(f"\n{success_files} files imported successfully into {success_tables} tables")

    # Transaction is committed
    connection.commit()


# sqlalchemy.base.Engine
def run_in_session(engine: Any, func: Callable[[Any], Any]) -> Any:
    """Run the given function within the scope of a single database connection session."""
    conn = engine.raw_connection()
    try:
        return func(conn)
    finally:
        conn.close()


def get_import_files_and_tables(
    directory: str, tables: Optional[List[str]], config_per_table: Optional[TablesConfig]
) -> Tuple[List[str], List[str]]:
    """Based on the configuration, determine the set of files to be imported as well as their destination tables."""
    if config_per_table is None:
        config_per_table = {}

    # Determine tables based on files in directory
    all_files = sorted([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])
    import_files = [f for f in all_files if re.match(r".*\.csv", f)]
    dest_tables = [f[: -len(".csv")] for f in import_files]

    # Consider subsets in config
    subsets = {
        table: [subset["name"] for subset in config_per_table[table]["subsets"]]
        for table in config_per_table
        if "subsets" in config_per_table[table]
    }
    subset_files = {filename: table for table in subsets for filename in subsets[table]}
    for subset_name in subset_files:
        filename = subset_name + ".csv"
        actual_table = subset_files[subset_name]
        if filename in import_files:
            # Update dest_tables with correct table
            dest_tables[import_files.index(filename)] = actual_table

    if tables is not None and len(tables) != 0:
        # Use only selected tables
        import_files = ["%s.csv" % (table,) for table in tables]
        dest_tables = tables

    # Check that all expected files exist
    expected_table_files = ["%s.csv" % (table,) for table in dest_tables]
    unknown_files = set(expected_table_files).difference(set(all_files))
    if len(unknown_files) > 0:
        print("No files found for the following tables:")
        for file in unknown_files:
            print("\t", file)
        sys.exit(EXIT_CODE_INVALID_DATA)

    # Convert filenames to full paths
    import_files = [os.path.join(directory, f) for f in import_files]
    return import_files, dest_tables


def validate_schema(inspector: Any, schema: str) -> str:
    """Check that the database schema specified exists and is valid."""
    if schema is None:  # pragma: no cover
        schema = inspector.default_schema_name
    if schema not in inspector.get_schema_names():
        print("Schema not found: '{}'".format(schema))
        sys.exit(EXIT_CODE_ARGS)
    return schema


def validate_tables(inspector: Any, schema: str, tables: Optional[List[str]]) -> Optional[List[str]]:
    """Check that the tables specified exists in the database."""
    if tables is None or len(tables) == 0:
        return None
    all_tables = set(inspector.get_table_names(schema))
    unknown_tables = set(tables) - all_tables
    if len(unknown_tables) > 0:
        print("Tables not found in database:")
        print("\t" + "\n\t".join(unknown_tables))
        sys.exit(EXIT_CODE_ARGS)
    return tables


def check_table_params(ctx: click.Context, param: Union[click.Option, click.Parameter], value: List[str]) -> List[str]:
    """Check that 'tables' have been specified if 'include-dependent-tables' CLI is provided."""
    assert param.name == "tables"
    other_flag = "include_dependent_tables"
    if len(value) == 0 and other_flag in ctx.params and ctx.params[other_flag] is True:
        raise click.UsageError(
            "Illegal usage: '{}' option is only valid if '{}' arguments have been specified.".format(
                other_flag, param.name
            )
        )
    return value


def load_table_config_or_exit(inspector: Any, schema: str, config_file_name: Optional[str]) -> Optional[TablesConfig]:
    """Load and validate table configuration and exit app if there are issues."""
    config_per_table = None
    if config_file_name is not None:
        try:
            config_per_table = load_config_for_tables(config_file_name)
            validate_table_configs_with_schema(inspector, schema, config_per_table)
        except ConfigInvalidException as exc:
            print(exc)
            sys.exit(EXIT_CODE_EXC)
    return config_per_table


def generate_single_table_config(
    directory: str, tables: List[str], config_per_table: Optional[TablesConfig]
) -> Tuple[List[str], List[str], TablesConfig]:
    """Create a fake config such that all files found in the directory are subsets for the given table."""
    assert len(tables) == 1
    table_name = tables[0]
    if config_per_table is None:
        config_per_table = {table_name: {}}

    all_files = sorted([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])
    import_files = [f for f in all_files if re.match(r".*\.csv", f)]

    # Add subsets to config if they don't already exist
    if "subsets" not in config_per_table[table_name]:
        config_per_table[table_name]["subsets"] = []
    current_subsets = [subset["name"] for subset in config_per_table[table_name]["subsets"]]
    for name in import_files:
        if name not in current_subsets:
            config_per_table[table_name]["subsets"].append({"name": name})

    dest_tables = [table_name] * len(import_files)
    import_files = [os.path.join(directory, f) for f in import_files]
    return import_files, dest_tables, config_per_table


# Shared command line options for connecting to a database
DB_CONNECT_OPTIONS = [
    click.option("--dbname", "-d", help="Database name to connect to.", required=True),
    click.option(
        "--host", "-h", help="Database server host or socket directory.", default="localhost", show_default=True
    ),
    click.option("--port", "-p", help="Database server port.", default="5432", show_default=True),
    click.option("--username", "-U", help="Database user name.", default="postgres", show_default=True),
    click.option("--schema", "-s", default="public", help="Database schema to use.", show_default=True),
    click.option("--no-password", "-w", is_flag=True, help="Never prompt for password (e.g. peer authentication)."),
    click.option(
        "--password",
        "-W",
        hide_input=True,
        prompt=False,
        default=None,
        help="Database password (default is to prompt for password or read config).",
    ),
    click.option(
        "--uri",
        "-L",
        help="Connection URI can be used instead of specifying parameters separately" + " (also sets --no-password).",
        required=False,
    ),
]

# Shared command line arguments for importing/exporting tables to a directory
DIR_TABLES_ARGUMENTS = [
    click.option(
        "--config",
        "-c",
        type=click.Path(exists=True, dir_okay=False),
        help="Config file for customizing how tables are imported/exported.",
    ),
    click.option(
        "--include-dependent-tables",
        "-i",
        is_flag=True,
        help="When selecting specific tables, also include "
        + "all tables on which they depend due to foreign key constraints.",
    ),
    click.argument("directory", nargs=1, type=click.Path(exists=True, file_okay=False)),
    click.argument("tables", default=None, nargs=-1, callback=check_table_params),
]


def version_callback(value: bool) -> None:
    """Print out application's version info."""
    if value:
        typer.echo(f"pgmerge, version {__version__}\nSimon Muller <samullers@gmail.com>")
        raise typer.Exit()


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Give more verbose output."),
    version: Optional[bool] = typer.Option(None, "--version", callback=version_callback, is_eager=True),
) -> None:
    """Use to add arguments related to whole app and not only specific sub-commands."""
    setup_logging(verbose)  # pragma: no cover


@click.command()
@decorate(DB_CONNECT_OPTIONS)
@decorate(DIR_TABLES_ARGUMENTS)
def export(
    dbname: str,
    uri: Optional[str],
    host: str,
    port: str,
    username: str,
    no_password: bool,
    password: Optional[str],
    schema: str,
    config: Optional[str],
    include_dependent_tables: bool,
    directory: str,
    tables: Optional[List[str]],
) -> None:
    """
    Export each table to a CSV file.

    If one or more tables are specified then only they will be used, otherwise all tables found will be selected. They
    will all be exported into the given directory.
    """
    engine = None
    try:
        if uri:
            no_password = True
        password = retrieve_password(APP_NAME, dbname, host, port, username, password, never_prompt=no_password)
        db_url = generate_url(uri, dbname, host, port, username, password)
        engine = sqlalchemy.create_engine(db_url)
        inspector = sqlalchemy.inspect(engine)
        schema = validate_schema(inspector, schema)
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = validate_tables(inspector, schema, tables)
        if include_dependent_tables and tables:
            tables = list(db_graph.get_all_dependent_tables(table_graph, tables))
        if tables is None:
            tables = sorted(inspector.get_table_names(schema))

        config_per_table = load_table_config_or_exit(inspector, schema, config)
        find_and_warn_about_cycles(table_graph, tables)

        def export_tables(conn: Any) -> Tuple[int, int]:
            return db_export.export_tables_per_config(
                conn, inspector, schema, directory, tables, config_per_table=config_per_table
            )

        table_count, file_count = run_in_session(engine, export_tables)
        print("Exported {} tables to {} files".format(table_count, file_count))
    except Exception as exc:  # pragma: no cover
        logging.exception(exc)
        sys.exit(EXIT_CODE_EXC)
    finally:
        if engine is not None:
            engine.dispose()


@click.command(name="import")
@decorate(DB_CONNECT_OPTIONS)
@click.option(
    "--ignore-cycles",
    "-f",
    is_flag=True,
    help="Don't stop import when cycles are detected in schema" + " (will still fail if there are cycles in the data)",
)
@click.option(
    "--disable-foreign-keys",
    "-F",
    is_flag=True,
    help="Disable foreign key constraint checking during import (necessary if you have cycles, but "
    + "requires superuser rights).",
)
@decorate(DIR_TABLES_ARGUMENTS)
@click.option(
    "--single-table",
    is_flag=True,
    help="An import-only option that assumes all files in the directory are the same type and imports "
    + "them all into a single table.",
)
def upsert(
    dbname: str,
    uri: Optional[str],
    host: str,
    port: str,
    username: str,
    no_password: bool,
    password: Optional[str],
    schema: str,
    config: Optional[str],
    include_dependent_tables: bool,
    ignore_cycles: bool,
    disable_foreign_keys: bool,
    single_table: bool,
    directory: str,
    tables: Optional[List[str]],
) -> None:
    """
    Import/merge each CSV file into a table.

    All CSV files need the same name as their matching table and have to be located in the given directory.
    If one or more tables are specified then only they will be used, otherwise all tables
    found will be selected.
    """
    engine = None
    try:
        if uri:
            no_password = True
        password = retrieve_password(APP_NAME, dbname, host, port, username, password, never_prompt=no_password)
        db_url = generate_url(uri, dbname, host, port, username, password)
        engine = sqlalchemy.create_engine(db_url)
        inspector = sqlalchemy.inspect(engine)
        schema = validate_schema(inspector, schema)
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema, tables=None)
        tables = validate_tables(inspector, schema, tables)
        if include_dependent_tables and tables:
            tables = list(db_graph.get_all_dependent_tables(table_graph, tables))

        if single_table and (tables is None or len(tables) == 0):
            print("One table has to be specified when using the --single-table option")
            sys.exit(EXIT_CODE_ARGS)
        tables = cast(List[str], tables)
        if single_table and len(tables) > 1:
            print("Only one table can be specified when using the --single-table option")
            sys.exit(EXIT_CODE_ARGS)

        config_per_table = load_table_config_or_exit(inspector, schema, config)
        if single_table:
            import_files, dest_tables, config_per_table = generate_single_table_config(
                directory, tables, config_per_table
            )
        else:
            import_files, dest_tables = get_import_files_and_tables(directory, tables, config_per_table)
        run_in_session(
            engine,
            lambda conn: import_all_new(
                conn,
                inspector,
                schema,
                import_files,
                dest_tables,
                config_per_table=config_per_table,
                suspend_foreign_keys=disable_foreign_keys,
                fail_on_warning=not ignore_cycles,
            ),
        )
    except Exception as exc:  # pragma: no cover
        logging.exception(exc)
        sys.exit(EXIT_CODE_EXC)
    finally:
        if engine is not None:
            engine.dispose()


@click.command(context_settings=dict(max_content_width=120))
@click.option("--engine", "-e", help="Type of database engine.", default="postgresql", show_default=True)
@decorate(DB_CONNECT_OPTIONS)
@click.option("--warnings", "-w", is_flag=True, help="Output any issues detected in database schema.")
@click.option("--list-tables", "-t", is_flag=True, help="Output all tables found in the given schema.")
@click.option(
    "--table-details", "-td", is_flag=True, help="Output all tables along with column and foreign key information."
)
@click.option("--cycles", "-c", is_flag=True, help="Find and list cycles in foreign-key dependency graph.")
@click.option(
    "--insert-order",
    "-i",
    is_flag=True,
    help="Output the insertion order of tables based on the foreign-key dependency graph. "
    + "This can be used by importer scripts if there are no circular dependency issues.",
)
@click.option("--partition", "-pt", is_flag=True, help="Partition and list sub-graphs of foreign-key dependency graph.")
@click.option(
    "--export-graph",
    "-x",
    is_flag=True,
    help="Output dot format description of foreign-key dependency graph."
    + " To use graphviz to generate a PDF from this format, pipe the output to:"
    + " dot -Tpdf > graph.pdf",
)
@click.option("--transferable", "-tf", is_flag=True, help="Output info related to table transfers.")
def inspect(
    engine: str,
    dbname: str,
    uri: Optional[str],
    host: str,
    port: str,
    username: str,
    no_password: bool,
    password: Optional[str],
    schema: str,
    warnings: bool,
    list_tables: bool,
    table_details: bool,
    partition: bool,
    cycles: bool,
    insert_order: bool,
    export_graph: bool,
    transferable: bool,
) -> None:
    """
    Inspect database schema in various ways.

    Defaults to PostgreSQL but should support multiple database engines thanks to SQLAlchemy (see:
    http://docs.sqlalchemy.org/en/latest/dialects/).
    """
    _engine = None
    try:
        if uri:
            no_password = True
        password = retrieve_password(APP_NAME, dbname, host, port, username, password, never_prompt=no_password)
        db_url = generate_url(uri, dbname, host, port, username, password, type=engine)
        _engine = sqlalchemy.create_engine(db_url)
        db_inspect.main(
            _engine,
            schema,
            warnings,
            list_tables,
            table_details,
            partition,
            cycles,
            insert_order,
            export_graph,
            transferable,
        )
    except Exception as exc:  # pragma: no cover
        logging.exception(exc)
        sys.exit(EXIT_CODE_EXC)
    finally:
        if _engine is not None:
            _engine.dispose()


# Typer/Click combination object
# cast() needed because Type is incorrectly defined in library?
cli_app = cast(click.Group, typer.main.get_command(app))
cli_app.add_command(upsert)
cli_app.add_command(export)
cli_app.add_command(inspect)


if __name__ == "__main__":
    cli_app()
