"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import logging

DEFAULT_FILE_FORMAT = "FORMAT CSV, HEADER, ENCODING 'UTF8'"
_log = logging.getLogger(__name__)


def log_sql(sql):
    _log.debug('SQL: {}'.format(sql))


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


def replace_indexes(listy, idxs_to_replace, new_values):
    """
    Remove given indexes and insert a new set of values into the given list.
    """
    # Delete values to be replaced (remove highest indices first so that indices don't change)
    for idx in reversed(sorted(idxs_to_replace)):
        del listy[idx]
    # We have to add all new values at the first index to be replaced since thats the only index which is now unchanged
    idx_to_add = min(idxs_to_replace)

    # Add multiple values in reverse so that we can keep the insertion index the same
    # and their final order will end up correct
    for value in reversed(new_values):
        listy.insert(idx_to_add, value)


def replace_local_columns_with_alternate_keys(inspector, config_per_table, schema, table, local_columns):
    """
    Create a list of foreign columns from a list of selected local columns of a table.
    Each foreign key column to a table with an alternate key will be replaced with columns for the alternate key.

    TODO: support multiple levels of indirection
    """
    foreign_columns = [(col, []) for col in local_columns]

    fks = inspector.get_foreign_keys(table, schema)
    for fk in fks:
        fk_columns = fk['constrained_columns']
        if not set(fk_columns).issubset(set(local_columns)):
            continue

        foreign_table = fk['referred_table']
        if foreign_table not in config_per_table:
            continue

        fk_table_config = config_per_table[foreign_table]
        if 'alternate_key' not in fk_table_config:
            continue
        new_columns = config_per_table[foreign_table]['alternate_key']

        foreign_column_names = [col[0] for col in foreign_columns]
        idxs_to_replace = [foreign_column_names.index(col) for col in fk_columns]
        new_values = [(col, [fk['name']]) for col in new_columns]
        replace_indexes(foreign_columns, idxs_to_replace, new_values)

    return foreign_columns


def export_columns(connection, inspector, schema, output_dir, tables, config_per_table=None, file_format=None):
    """
    Exports all given tables with the columns specified in the columns_per_table dictionary.
    """
    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT
    if config_per_table is None:
        config_per_table = {}

    cursor = connection.cursor()

    for table in tables:
        if table not in config_per_table or config_per_table[table] is None:
            config_per_table[table] = {}

        if 'columns' in config_per_table[table]:
            local_columns = config_per_table[table]['columns']
        else:
            local_columns = [col['name'] for col in inspector.get_columns(table, schema)]

        foreign_columns = replace_local_columns_with_alternate_keys(inspector, config_per_table, schema, table, local_columns)
        where_clause = None
        if 'where' in config_per_table[table]:
            where_clause = config_per_table[table]['where']

        order_columns = get_unique_columns(inspector, table, schema)
        output_file = os.path.join(output_dir, table + '.csv')
        export_table_with_any_columns(cursor, inspector, output_file, schema, table,
                                      any_columns=foreign_columns, order_columns=order_columns,
                                      file_format=file_format, where_clause=where_clause)

    connection.commit()


def sql_join_from_foreign_key(foreign_key, table_or_alias, join_alias=None,
                              local_columns_key='constrained_columns', foreign_columns_key='referred_columns'):
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


def sql_join_alias_for_foreign_key(foreign_key):
    return 'join_{}'.format(foreign_key['name'])


def sql_select_table_with_foreign_columns(inspector, schema, table, foreign_columns=None, order_columns=None,
                                          alias_columns=True, where_clause=None):
    """
    :param foreign_columns: A list of tuples describing which columns to export. Columns can be from any other tables
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


def export_table_with_any_columns(cursor, inspector, output_path, schema, main_table,
                                  any_columns=None, order_columns=None, file_format=None, where_clause=None):
    """
    Exports a single table with any of the specified columns. Columns could be in the table or any of it's dependencies.
    """
    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT

    select_sql = sql_select_table_with_foreign_columns(inspector, schema, main_table, any_columns, order_columns,
                                                       where_clause=where_clause)
    copy_sql = 'COPY ({select_sql}) TO STDOUT WITH ({file_format})'\
        .format(select_sql=select_sql, file_format=file_format)
    log_sql(copy_sql)

    with open(output_path, 'wb') as output_file:
        cursor.copy_expert(copy_sql, output_file)


class ExportException(Exception):
    """
    Exception raised for errors detected before or during export.
    """

    def __init__(self, message):
        super().__init__(message)
