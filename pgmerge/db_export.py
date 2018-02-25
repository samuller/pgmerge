"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import logging

_log = logging.getLogger(__name__)


def log_sql(sql):
    _log.debug('SQL: {}'.format(sql))


def export_columns(connection, inspector, schema, output_dir, tables, columns_per_table=None, file_format=None):
    if file_format is None:
        file_format = "FORMAT CSV, HEADER, ENCODING 'UTF8'"

    cursor = connection.cursor()

    for table in tables:
        all_columns = inspector.get_columns(table, schema)
        pks = inspector.get_primary_keys(table, schema)

        columns_str = '*'
        if columns_per_table is not None and table in columns_per_table and columns_per_table[table] is not None:
            columns_str = ','.join(columns_per_table[table])

        order_str = ''
        order_columns = [col['name'] for col in all_columns]
        if len(pks) > 0:
            order_columns = pks
        if len(order_columns) > 0:
            order_str = ' ORDER BY ' + ','.join(order_columns)

        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY (SELECT {columns_str} from {schema}.{table}{order_str}) TO STDOUT WITH ({file_format})'\
            .format(columns_str=columns_str, schema=schema, table=table,
                    order_str=order_str, file_format=file_format)
        log_sql(copy_sql)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()
