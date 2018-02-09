import os
from . import db_graph


def export_all(connection, inspector, schema, output_dir, tables=None, file_format="FORMAT CSV, HEADER"):
    cursor = connection.cursor()

    for table in tables:
        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY %s TO STDOUT WITH (%s)' % (table, file_format)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()
