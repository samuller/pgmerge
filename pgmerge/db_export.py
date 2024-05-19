"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import os
import logging
from typing import Any, List, Tuple, Optional, cast

from .utils import replace_indexes
from .db_config import TablesConfig, SubsetConfig

DEFAULT_FILE_FORMAT = "FORMAT CSV, HEADER, ENCODING 'UTF8'"
_log = logging.getLogger(__name__)


ForeignColumnPath = Tuple[str, List[str]]


def _log_sql(sql: str) -> None:
    _log.debug('SQL: {}'.format(sql))


def get_unique_columns(inspector: Any, table: str, schema: str) -> List[str]:
    """
    Get all columns in table that have constraints forcing uniqueness.

    If the combination of primary key and unique constraints is used to identify a row, then you'll miss rows where
    values in separate unique constraints have been swapped. This means that extra INSERTS or missed UPDATES could
    happen if these columns are collectively used as an identifier.
    """
    pks = cast(List[str], inspector.get_pk_constraint(table, schema)['constrained_columns'])
    unique_constraints = inspector.get_unique_constraints(table, schema)
    unique = [col for constraint in unique_constraints for col in constraint['column_names']]
    return pks + unique


def replace_local_columns_with_alternate_keys(inspector: Any, config_per_table: TablesConfig, schema: str,
                                              table: str, local_columns: List[str]
                                              ) -> List[ForeignColumnPath]:
    """
    Replace foreign-key columns in table with alternate key columns of foreign table.

    Create a list of foreign columns from a list of selected local columns of a table.
    Each foreign key column to a table with an alternate key will be replaced with columns for the alternate key.

    TODO: support multiple levels of indirection
    """
    foreign_columns: List[ForeignColumnPath] = [(col, []) for col in local_columns]

    fks = inspector.get_foreign_keys(table, schema)
    for fky in fks:
        fk_columns = fky['constrained_columns']
        if not set(fk_columns).issubset(set(local_columns)):
            continue

        foreign_table = fky['referred_table']
        if foreign_table not in config_per_table:
            continue

        fk_table_config = config_per_table[foreign_table]
        if 'alternate_key' not in fk_table_config:
            continue
        new_columns = config_per_table[foreign_table]['alternate_key']

        foreign_column_names = [col[0] for col in foreign_columns]
        idxs_to_replace = [foreign_column_names.index(col) for col in fk_columns]
        new_values = [(col, [fky['name']]) for col in new_columns]
        replace_indexes(foreign_columns, idxs_to_replace, new_values)

    return foreign_columns


def export_tables_per_config(connection: Any, inspector: Any, schema: str, output_dir: str, tables: List[str],
                             config_per_table: Optional[TablesConfig] = None,
                             file_format: Optional[str] = None) -> Tuple[int, int]:
    """Export all given tables according to the options specified in the config_per_table dictionary."""
    if connection.encoding != 'UTF8':
        # raise ExportException('Database connection encoding isn\'t UTF8: {}'.format(connection.encoding))
        print("WARNING: Setting database connection encoding to UTF8 instead of '{}'".format(connection.encoding))
        connection.set_client_encoding('UTF8')

    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT
    if config_per_table is None:
        config_per_table = {}

    cursor = connection.cursor()
    file_count = 0
    for table in tables:
        if table not in config_per_table or config_per_table[table] is None:
            config_per_table[table] = {}

        # Determine files to be generated: one per table plus one for each of its subsets
        file_configs = [cast(SubsetConfig, config_per_table[table])]
        file_configs[0]['name'] = table

        if 'subsets' in config_per_table[table]:
            file_configs.extend(config_per_table[table]['subsets'])
            # Propagate parent's "columns" config to all subsets that haven't defined it
            column_config = config_per_table[table].get('columns')
            if column_config is not None:
                for file_config in file_configs:
                    if file_config.get('columns') is None:
                        file_config['columns'] = column_config

        for file_config in file_configs:
            if 'columns' in file_config:
                local_columns = file_config['columns']
            else:
                local_columns = [col['name'] for col in inspector.get_columns(table, schema)]
            foreign_columns = replace_local_columns_with_alternate_keys(inspector, config_per_table, schema,
                                                                        table, local_columns)
            where_clause = file_config.get('where')
            order_columns = get_unique_columns(inspector, table, schema)
            # Remove columns that are not selected to be part of export
            order_columns_to_remove = list(set(order_columns).difference(set(local_columns)))
            if len(order_columns_to_remove) > 0:
                order_columns = [col for col in order_columns if col not in order_columns_to_remove]
            output_file = os.path.join(output_dir, file_config['name'] + '.csv')
            export_table_with_any_columns(cursor, inspector, output_file, schema, table,
                                          any_columns=foreign_columns, order_columns=order_columns,
                                          file_format=file_format, where_clause=where_clause)
        file_count += len(file_configs)

    connection.commit()
    return len(tables), file_count


def sql_join_from_foreign_key(foreign_key: Any, table_or_alias: str, join_alias: Optional[str] = None,
                              local_columns_key: str = 'constrained_columns',
                              foreign_columns_key: str = 'referred_columns') -> str:
    """Create SQL to join with table using foreign key."""
    assert local_columns_key in foreign_key
    assert foreign_columns_key in foreign_key
    assert len(foreign_key[local_columns_key]) == len(foreign_key[foreign_columns_key])
    if join_alias is None:
        join_alias = sql_join_alias_for_foreign_key(foreign_key)
    comparisons = []
    for col, ref_col in zip(foreign_key[local_columns_key], foreign_key[foreign_columns_key]):
        comparisons.append('({t}.{c} = {rt}.{rc} OR ({t}.{c} IS NULL AND {rt}.{rc} IS NULL))'.format(
            t=table_or_alias, c=col, rt=join_alias, rc=ref_col
        ))
    return "LEFT JOIN {referred_schema}.{referred_table} AS {join_alias} ON {cmps}"\
        .format(join_alias=join_alias, cmps=" AND ".join(comparisons), **foreign_key)


def sql_join_alias_for_foreign_key(foreign_key: Any) -> str:
    """Create SQL to create a unique alias for table being joined."""
    return 'join_{}'.format(foreign_key['name'])


def sql_select_table_with_foreign_columns(inspector: Any, schema: str, table: str,
                                          foreign_columns: Optional[List[ForeignColumnPath]] = None,
                                          order_columns: Optional[List[str]] = None,
                                          alias_columns: bool = True, where_clause: Optional[str] = None
                                          ) -> str:
    """
    Create SQL to select a table, but with it's own columns replaced with those from foreign tables.

    Parameters
    ----------
    foreign_columns :
        A list of tuples describing which columns to export. Columns can be from any other tables
        that are dependencies of this one. Each tuple should be of the format:
            (column_name, list_of_foreign_key_names_to_reach_table_with_column)
        To use a column from the current table, use:
            (column_name, [])
        A column from a directly linked table would be based on the name of the foreign key link:
            (column_name, [fk_name])
    """
    if foreign_columns is None:
        all_columns = inspector.get_columns(table, schema)
        foreign_columns = [(col['name'], []) for col in all_columns]

    all_fks = inspector.get_foreign_keys(table, schema)
    # TODO: consider if all foreign key columns always be exported?
    # foreign_columns.extend([(fk['referred_columns'][0], [fk['name']]) for fk in all_fks])

    per_column_sql = []
    per_join_sql = []
    for column_name, foreign_key_path in foreign_columns:
        prev_fk_alias = table
        foreign_table_fks_by_name = {fk['name']: fk for fk in all_fks}
        for foreign_key_name in foreign_key_path:
            if foreign_key_name not in foreign_table_fks_by_name:
                raise ExportException('Unknown foreign key {} found in path {} of provided columns: {}'
                                      .format(foreign_key_name, foreign_key_path, foreign_columns))
            foreign_key = foreign_table_fks_by_name[foreign_key_name]
            per_join_sql.append(sql_join_from_foreign_key(foreign_key, prev_fk_alias))
            # For next iteration
            foreign_table_fks_by_name = {fk['name']: fk for fk in inspector.get_foreign_keys(
                foreign_key['referred_table'], foreign_key['referred_schema'])}
            prev_fk_alias = sql_join_alias_for_foreign_key(foreign_key)

        alias_sql = ''
        if alias_columns and prev_fk_alias != table:
            alias_sql = ' AS {join_alias}_{column}'.format(join_alias=prev_fk_alias, column=column_name)

        per_column_sql.append('{join_alias}.{column}{alias_sql}'.format(
            join_alias=prev_fk_alias, column=column_name, alias_sql=alias_sql))

    joins_sql = ' ' + ' '.join(set(per_join_sql))
    columns_sql = ', '.join(per_column_sql)
    order_sql = ''
    if order_columns is not None and len(order_columns) > 0:
        order_sql = ' ORDER BY ' + ','.join(order_columns)

    where_sql = ''
    if where_clause is not None:
        where_sql = ' WHERE ' + where_clause

    select_sql = 'SELECT {columns_sql} from {schema}.{main_table}{joins_sql}{where_sql}{order_sql}' \
        .format(columns_sql=columns_sql, schema=schema, main_table=table, joins_sql=joins_sql,
                where_sql=where_sql, order_sql=order_sql)

    return select_sql


def export_table_with_any_columns(cursor: Any, inspector: Any, output_path: str, schema: str, main_table: str,
                                  any_columns: Optional[List[ForeignColumnPath]] = None,
                                  order_columns: Optional[List[str]] = None,
                                  file_format: Optional[str] = None, where_clause: Optional[str] = None
                                  ) -> None:
    """
    Export a single table with any of the specified columns.

    Columns could be in the table or any of its dependencies.
    """
    if file_format is None:  # pragma: no cover
        file_format = DEFAULT_FILE_FORMAT

    select_sql = sql_select_table_with_foreign_columns(inspector, schema, main_table, any_columns, order_columns,
                                                       where_clause=where_clause)
    copy_sql = 'COPY ({select_sql}) TO STDOUT WITH ({file_format})'\
        .format(select_sql=select_sql, file_format=file_format)
    _log_sql(copy_sql)

    with open(output_path, 'wb') as output_file:
        cursor.copy_expert(copy_sql, output_file)


class ExportException(Exception):
    """Exception raised for errors detected before or during export."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
