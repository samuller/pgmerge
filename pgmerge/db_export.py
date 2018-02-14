"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import os
import logging

log = logging.getLogger(__name__)


def log_sql(sql):
    log.debug('SQL: {}'.format(sql))


def export_all(connection, schema, output_dir, tables, file_format=None):
    if file_format is None:
        file_format = "FORMAT CSV, HEADER, ENCODING 'UTF8'"

    cursor = connection.cursor()

    for table in tables:
        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY %s.%s TO STDOUT WITH (%s)' % (schema, table, file_format)
        log_sql(copy_sql)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()
