"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import logging

_log = logging.getLogger(__name__)


def log_sql(sql):
    _log.debug('SQL: {}'.format(sql))


def export_columns(connection, schema, output_dir, tables, columns_per_table=None, file_format=None):
    if file_format is None:
        file_format = "FORMAT CSV, HEADER, ENCODING 'UTF8'"

    cursor = connection.cursor()

    for table in tables:
        columns_str = '*'
        if columns_per_table is not None and table in columns_per_table and columns_per_table[table] is not None:
            columns_str = ','.join(columns_per_table[table])

        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY (SELECT {} from {}.{}) TO STDOUT WITH ({})'.format(
            columns_str, schema, table, file_format)
        log_sql(copy_sql)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()
