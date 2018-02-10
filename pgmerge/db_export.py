import os


def export_all(connection, schema, output_dir, tables=None, file_format="FORMAT CSV, HEADER"):
    cursor = connection.cursor()

    for table in tables:
        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY %s.%s TO STDOUT WITH (%s)' % (schema, table, file_format)
        cursor.copy_expert(copy_sql, output_file)

    connection.commit()
