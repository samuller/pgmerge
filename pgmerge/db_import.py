"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import logging
from typing import Any, List, Dict, Tuple, Optional, cast

from .utils import replace_indexes
from .db_config import TablesConfig, FileConfig
from .db_export import ForeignColumnPath, get_unique_columns, \
    replace_local_columns_with_alternate_keys, \
    sql_select_table_with_foreign_columns, \
    sql_join_alias_for_foreign_key, sql_join_from_foreign_key

# 3.8+: Literal['skip', 'insert', 'update', 'total']
ImportStats = Dict[str, int]

_log = logging.getLogger(__name__)


def _log_sql(sql: str) -> None:
    _log.debug('SQL: {}'.format(sql))


def exec_sql(cursor: Any, sql: str) -> None:
    """Execute the given SQL."""
    _log_sql(sql)
    cursor.execute(sql)


def sql_delete_identical_rows_between_tables(delete_table_name: str, reference_table_name: str,
                                             all_column_names: List[str]) -> str:
    """Create SQL to delete rows from a table that are identical to rows in a reference table."""
    # "IS NOT DISTINCT FROM" handles NULLS better (even composite type columns), but is not indexed
    # where_clause = " AND ".join(["%s.%s IS NOT DISTINCT FROM %s.%s" % (table, col, temp_table_name, col)
    #                               for col in all_columns])
    where_clause = " AND ".join(
        ["({ref}.{col} = {dlt}.{col} OR ({ref}.{col} IS NULL AND {dlt}.{col} IS NULL))".format(
            ref=reference_table_name, col=col, dlt=delete_table_name) for col in all_column_names])

    delete_sql = "DELETE FROM {dlt} USING {ref} WHERE {where_clause};".format(
        dlt=delete_table_name, ref=reference_table_name, where_clause=where_clause)
    return delete_sql


def sql_insert_rows_not_in_table(insert_table_name: str, reference_table_name: str, id_column_names: List[str],
                                 column_names: List[str]) -> str:
    """Create SQL to insert rows into a table, but only if those rows don't already exist in a reference table."""
    insert_table_cols = ",".join(["{tbl}.{col}".format(tbl=insert_table_name, col=col)
                                  for col in id_column_names])
    reference_table_cols = ",".join(["_tft.{col}".format(col=col)
                                     for col in id_column_names])
    # Use sub-select with extra column to maintain row order.
    subselect_sql = f"SELECT ROW_NUMBER() OVER () as __row_number, * FROM {reference_table_name}"
    tft_columns = ','.join([f"_tft.{col}" for col in column_names])
    # The left join will give nulls for the joined table when no matches are found.
    # We use '(tuple) is null' to see if all columns (values in the tuple) are null.
    select_sql = "SELECT {tft_cols} FROM ({ref}) as _tft LEFT JOIN {ins} ON ({ins_cols}) = ({ref_cols}) " \
        "WHERE ({ins_cols}) is NULL ORDER BY _tft.__row_number" \
        .format(tft_cols=tft_columns, ref=subselect_sql, ins=insert_table_name,
                ins_cols=insert_table_cols, ref_cols=reference_table_cols)
    columns_sql = ','.join(column_names)

    insert_sql = "INSERT INTO {ins}({cols}) ({select_sql}) RETURNING NULL;".format(
        ins=insert_table_name, cols=columns_sql, select_sql=select_sql)
    return insert_sql


def sql_update_rows_between_tables(update_table_name: str, reference_table_name: str, id_column_names: List[str],
                                   all_column_names: List[str]) -> str:
    """Create SQL to update rows in a table with values from a reference table."""
    # UPDATE table_b SET column1 = a.column1, column2 = a.column2, column3 = a.column3
    # FROM table_a WHERE table_a.id = table_b.id AND table_b.id in (1, 2, 3)
    set_columns = ",".join(["{} = {}.{}".format(col, reference_table_name, col)
                            for col in all_column_names])
    where_clause = " AND ".join(["{}.{} = {}.{}".format(update_table_name, col, reference_table_name, col)
                                 for col in id_column_names])
    update_sql = "UPDATE {upd} SET {set_columns} FROM {ref} WHERE {where_clause};".format(
        upd=update_table_name, set_columns=set_columns, ref=reference_table_name, where_clause=where_clause)
    return update_sql


def pg_upsert(inspector: Any, cursor: Any, schema: str, dest_table: str, input_file: str,
              file_format: Optional[str] = None, file_config: Optional[FileConfig] = None,
              config_per_table: Optional[TablesConfig] = None) -> ImportStats:
    """
    Do a full import (actually a merge or upsert) of a single file into a single table.

    Postgresql 9.5+ includes merge/upsert with INSERT ... ON CONFLICT, but it requires columns to have unique
    constraints (or even a partial unique index). We might use it once we're sure that it covers all our use cases.

    The import steps are as follows:
    - Create temporary table that matches columns of CSV and use COPY to import data
    - Create another temporary table that matches columns of the destination table
    - Transform data and copy it to the second temporary table
    - Compare data in second temporary table and destination table and only import/update the necessary rows/fields

    Parameters
    ----------
    file_config :
        Config for the file being imported
    config_per_table :
        Config for all tables. Will be used in case of foreign keys to other tables. Also used if file_config is None.

    Returns
    -------
    A dictionary of import/update/skip stats.
    """
    ########
    # Set default values
    ########
    # Set default values for parameters
    file_format = "FORMAT CSV, HEADER, ENCODING 'UTF8'" if file_format is None else file_format
    config_per_table = {} if config_per_table is None else config_per_table
    file_config = cast(FileConfig, config_per_table.get(dest_table, {}) if file_config is None else file_config)
    # Load values from config or set defaults
    columns = file_config.get('columns', None)
    all_columns = [col['name'] for col in inspector.get_columns(dest_table, schema)]
    columns = all_columns if columns is None else columns
    alternate_key = file_config.get('alternate_key', None)
    id_columns = get_unique_columns(inspector, dest_table, schema) if alternate_key is None else alternate_key

    ########
    # Check validity of parameters
    ########
    # Table should either be setup correctly, or alternate key should be specified
    if len(id_columns) == 0:
        raise UnsupportedSchemaException("Table has no primary key or unique columns!")
    unknown_columns = set(columns) - set(all_columns)
    if len(unknown_columns) > 0:
        raise InputParametersException("Columns provided do not exist in table '{}': {}"
                                       .format(dest_table, unknown_columns))
    skipped_id_columns = set(id_columns) - set(columns)
    if len(skipped_id_columns) > 0:
        raise InputParametersException("Columns provided do not include required id"
                                       " columns for table '{}': {}".format(dest_table, skipped_id_columns))

    ########
    # Create and import data into first (input) temporary table
    ########
    stats: ImportStats = {'skip': 0, 'insert': 0, 'update': 0, 'total': 0}

    table_name_tmp_copy = "_tmp_copy_{}".format(dest_table)
    foreign_columns = replace_local_columns_with_alternate_keys(inspector, config_per_table,
                                                                schema, dest_table, columns)
    select_sql = sql_select_table_with_foreign_columns(inspector, schema, dest_table, foreign_columns)
    # Create temporary table with same columns and types as target table
    create_sql = "CREATE TEMP TABLE {tmp_copy} AS {select_sql} LIMIT 0;".format(
        tmp_copy=table_name_tmp_copy, select_sql=select_sql)
    exec_sql(cursor, create_sql)

    # Import data into temporary table
    copy_sql = 'COPY {tbl} FROM STDOUT WITH ({format});'.format(tbl=table_name_tmp_copy, format=file_format)
    _log_sql(copy_sql)
    with open(input_file, 'r', encoding="utf-8") as file:
        cursor.copy_expert(copy_sql, file)
    stats['total'] = cursor.rowcount

    # Run analyze to improve performance after populating temporary table.
    # See: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-TEMPORARY
    # and: https://www.postgresql.org/docs/current/populate.html#POPULATE-ANALYZE
    analyze_sql = "ANALYZE {tmp_copy}".format(tmp_copy=table_name_tmp_copy)
    exec_sql(cursor, analyze_sql)

    ########
    # Create second (output) temporary table and transform and insert data
    ########
    # select_sql = sql_select_table_with_foreign_columns(inspector, schema, dest_table)
    table_name_tmp_final = "_tmp_final_{}".format(dest_table)
    select_sql = sql_select_table_with_local_columns(inspector, schema, dest_table,
                                                     table_name_tmp_copy, foreign_columns,
                                                     config_per_table)
    create_sql = "CREATE TEMP TABLE {tmp_final} AS {select_sql};".format(
        tmp_final=table_name_tmp_final, select_sql=select_sql)
    exec_sql(cursor, create_sql)
    # Add index so that comparison for identical rows is much faster
    index_sql = "CREATE INDEX ON {} ({});".format(table_name_tmp_final,
                                                  ",".join(id_columns))
    exec_sql(cursor, index_sql)

    upsert_stats = upsert_table_to_table(cursor, table_name_tmp_final, dest_table, id_columns, columns)
    stats.update(upsert_stats)

    ########
    # Clean-up
    ########
    drop_sql = "DROP TABLE {};".format(table_name_tmp_copy)
    exec_sql(cursor, drop_sql)

    drop_sql = "DROP TABLE {};".format(table_name_tmp_final)
    exec_sql(cursor, drop_sql)

    # Run analyze to improve performance after populating table.
    analyze_sql = "ANALYZE {}".format(dest_table)
    exec_sql(cursor, analyze_sql)

    return stats


def upsert_table_to_table(cursor: Any, src_table: str, dest_table: str, id_columns: List[str], columns: List[str]
                          ) -> ImportStats:
    """Do a full upsert import from a source table to a destination table."""
    stats: ImportStats = {'skip': 0, 'insert': 0, 'update': 0}

    # Delete rows in temp table that are already identical to those in destination table
    exec_sql(cursor, sql_delete_identical_rows_between_tables(src_table, dest_table, columns))
    stats['skip'] = cursor.rowcount

    # Insert rows from temp table that are not in destination table (according to id columns)
    exec_sql(cursor, sql_insert_rows_not_in_table(dest_table, src_table, id_columns, columns))
    stats['insert'] = cursor.rowcount
    # Delete rows that were just inserted
    exec_sql(cursor, sql_delete_identical_rows_between_tables(src_table, dest_table, columns))

    # Update rows whose id columns match in destination table
    exec_sql(cursor, sql_update_rows_between_tables(dest_table, src_table, id_columns, columns))
    stats['update'] = cursor.rowcount

    return stats


def sql_joins_for_each_path(paths: List[Tuple[str, ...]], src_table: str, fks_with_join_columns_by_name: Dict[str, Any]
                            ) -> List[str]:
    """Create SQL joins for each step in the list of given path lists."""
    per_join_sql = []
    for path in paths:
        if len(path) == 0:
            continue
        elif len(path) == 1:
            cur_table = src_table
        else:
            cur_table = sql_join_alias_for_foreign_key(path[-2])

        final_fk = fks_with_join_columns_by_name[path[-1]]
        # TODO: consider if join using only last reference can work when foreign key path is known
        per_join_sql.append(sql_join_from_foreign_key(final_fk, cur_table,
                                                      local_columns_key='join_columns_local',
                                                      foreign_columns_key='join_columns_foreign'))
    return per_join_sql


def replace_foreign_columns_with_local_columns(foreign_columns: List[ForeignColumnPath],
                                               fks_by_name: Dict[str, Any], src_table: str
                                               ) -> List[ForeignColumnPath]:
    """
    Replace "foreign columns" from file data with the corresponding columns of the import table.

    Foreign columns are columns from other tables that are referenced by columns with foreign keys in
    the current table being imported to.
    """
    fks = set()
    for _, path in foreign_columns:
        if len(path) == 1:
            fks.add(path[-1])

    for fk_name in fks:
        fk = fks_by_name[fk_name]
        join_alias = sql_join_alias_for_foreign_key(fk)

        idxs_to_replace = [idx for idx, fc in enumerate(foreign_columns) if fk_name in fc[1]]
        fk_sql_names = ["{join_alias}.{ref_col} AS {con_col}".format(
            join_alias=join_alias,
            ref_col=fk['referred_columns'][idx],
            con_col=fk['constrained_columns'][idx]) for idx in range(len(fk['referred_columns']))]
        new_values = [(name, [fk['name']]) for name in fk_sql_names]

        replace_indexes(foreign_columns, idxs_to_replace, new_values)

    for idx in range(len(foreign_columns)):
        col, path = foreign_columns[idx]
        if len(path) == 0:
            foreign_columns[idx] = ("{}.{}".format(src_table, col), [])

    return foreign_columns


def sql_select_table_with_local_columns(inspector: Any, schema: str, schema_table: Any, src_table: str,
                                        foreign_columns: List[ForeignColumnPath],
                                        local_columns_subset: Any = None,
                                        config_per_table: Optional[TablesConfig] = None) -> str:
    """
    Create SQL to convert src_table's foreign columns to local columns matching those of the schema_table.

    The foreign columns should be based on the foreign keys from the schema
    table (in the case of further indirection, some other tables will also be included).

    Parameters
    ----------
    schema_table :
        Has foreign keys
    src_table :
        Will be selected from and might be a temporary table (i.e. no schema)
    """
    if config_per_table is None:
        config_per_table = {}

    # Check correctness of paths and build up all foreign keys possibly needed
    all_fks = inspector.get_foreign_keys(schema_table, schema)
    fks_by_name = {fk['name']: fk for fk in all_fks}

    grouped_foreign_columns = {tuple(path): path for _, path in foreign_columns}
    paths = list(grouped_foreign_columns.keys())
    paths.sort(key=lambda path: len(path))
    idxs_by_fk: Dict[str, List[int]] = {}
    for idx, path in enumerate(paths):
        if len(path) == 0:
            continue
        if path[-1] not in fks_by_name:
            # To be able to join to path [fk1, fk2, fk3] we also need path [fk1, fk2] somewhere
            # Assumes columns for a path [fk1, fk2] will always be before columns for [fk1, fk2, fk3]
            raise InputParametersException("Partial path missing for: {}".format(path))
        idxs_by_fk.setdefault(path[-1], []).append(idx)

        final_fk = fks_by_name[path[-1]]
        new_fks = inspector.get_foreign_keys(final_fk['referred_table'], schema)
        fks_by_name.update({fk['name']: fk for fk in new_fks})

    # Go through all foreign columns and collect all 'replaced columns'
    for column, fpath in foreign_columns:
        if len(fpath) == 0:
            continue
        final_fk = fks_by_name[fpath[-1]]
        join_alias = sql_join_alias_for_foreign_key(final_fk)
        final_fk.setdefault('join_columns_local', []).append(
            "{join_alias}_{column}".format(join_alias=join_alias, column=column))
        final_fk.setdefault('join_columns_foreign', []).append(column)

    # Create joins for all foreign keys
    per_join_sql = sql_joins_for_each_path(paths, src_table, fks_by_name)

    # Replace foreign key values
    foreign_columns = replace_foreign_columns_with_local_columns(foreign_columns, fks_by_name, src_table)

    joins_sql = " " + " ".join(per_join_sql)
    columns_sql = ','.join([col for col, path in foreign_columns])

    # We don't use {schema}.{src_table} since that doesn't allow temporary tables
    return "SELECT {columns_sql} FROM {src_table}{joins_sql}".format(
        columns_sql=columns_sql, src_table=src_table, joins_sql=joins_sql)


def disable_foreign_key_constraints(cursor: Any) -> None:
    """
    Disable database checking of foreign key constraints.

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
    exec_sql(cursor, sql)


def enable_foreign_key_constraints(cursor: Any) -> None:
    """Enable database checking of foreign key constraints."""
    sql = "SET session_replication_role = DEFAULT;"
    exec_sql(cursor, sql)


class PreImportException(Exception):
    """Exception raised for errors detected before starting import."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class UnsupportedSchemaException(PreImportException):
    """Exception raised due to database schema being unsupported by import."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InputParametersException(PreImportException):
    """Exception raised due to incorrect parameters provided to import."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
