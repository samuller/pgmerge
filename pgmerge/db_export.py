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


def export_table(cursor, inspector, schema, output_path, table, columns=None, file_format=None):
    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT

    all_columns = inspector.get_columns(table, schema)
    pks = inspector.get_primary_keys(table, schema)

    columns_str = '*'
    if columns is not None:
        columns_str = ','.join(columns)

    order_str = ''
    order_columns = [col['name'] for col in all_columns]
    if len(pks) > 0:
        order_columns = pks
    if len(order_columns) > 0:
        order_str = ' ORDER BY ' + ','.join(order_columns)

    output_file = open(output_path, 'wb')
    copy_sql = 'COPY (SELECT {columns_str} from {schema}.{table}{order_str}) TO STDOUT WITH ({file_format})'\
        .format(columns_str=columns_str, schema=schema, table=table,
                order_str=order_str, file_format=file_format)
    log_sql(copy_sql)
    cursor.copy_expert(copy_sql, output_file)


def export_columns(connection, inspector, schema, output_dir, tables, columns_per_table=None, file_format=None):
    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT

    cursor = connection.cursor()

    for table in tables:
        columns = None
        if columns_per_table is not None and table in columns_per_table and columns_per_table[table] is not None:
            columns = columns_per_table[table]
        output_file = os.path.join(output_dir, table + '.csv')
        export_table(cursor, inspector, schema, output_file, table, columns, file_format=file_format)

    connection.commit()


def sql_join_from_foreign_key(foreign_key, table, join_alias=None):
    assert len(foreign_key['constrained_columns']) == len(foreign_key['referred_columns'])
    if join_alias is None:
        join_alias = sql_join_alias_for_foreign_key(foreign_key)
    comparisons = []
    for col, ref_col in zip(foreign_key['constrained_columns'], foreign_key['referred_columns']):
        comparisons.append('({t}.{c} = {rt}.{rc} OR ({t}.{c} IS NULL AND {rt}.{rc} IS NULL))'.format(
            t=table, c=col, rt=join_alias, rc=ref_col
        ))
    return "LEFT JOIN {referred_schema}.{referred_table} AS {join_alias} ON {cmps}"\
        .format(**foreign_key, join_alias=join_alias, cmps=" AND ".join(comparisons))


def sql_join_alias_for_foreign_key(foreign_key):
    return 'join_{}'.format(foreign_key['name'])


def export_alternate_keys(cursor, inspector, output_path, schema, main_table,
                          table_columns=None, order_columns=None, file_format=None):
    if file_format is None:
        file_format = DEFAULT_FILE_FORMAT

    fks = inspector.get_foreign_keys(main_table, schema)
    # Joins TODO: Add more joins based on alternate keys of joined tables
    joins_sql = " " + " ".join([sql_join_from_foreign_key(fk, main_table) for fk in fks])
    # Columns
    join_columns = ["{0}.{1} AS fk_{2}_{1}".format(sql_join_alias_for_foreign_key(fk), col, fk['name'])
                    for fk in fks for col in fk['referred_columns']]
    columns_sql = '{}.*'.format(main_table)
    if len(join_columns) > 0:
        columns_sql += ', ' + ', '.join(join_columns)
    # Order
    order_sql = ''
    if order_columns is not None and len(order_columns) > 0:
        order_sql = ' ORDER BY ' + ','.join(order_columns)

    output_file = open(output_path, 'wb')

    select_sql = 'SELECT {columns_sql} from {schema}.{main_table}{joins_sql}{order_sql}'\
        .format(columns_sql=columns_sql, schema=schema, main_table=main_table, joins_sql=joins_sql, order_sql=order_sql)
    copy_sql = 'COPY ({select_sql}) TO STDOUT WITH ({file_format})'\
        .format(columns_str=columns_sql, select_sql=select_sql, file_format=file_format)
    log_sql(copy_sql)
    cursor.copy_expert(copy_sql, output_file)
