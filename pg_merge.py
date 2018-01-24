#!/usr/bin/env python3
import os
import click
from sqlalchemy import create_engine, inspect

found_config = True
try:
    import config
except ImportError:
    found_config = False


def export_all(engine, inspector, schema, output_dir, file_format="CSV HEADER"):
    fake_conn = engine.raw_connection()
    fake_cur = fake_conn.cursor()

    tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        output_file = open(os.path.join(output_dir, table + '.csv'), 'wb')
        copy_sql = 'COPY %s TO STDOUT WITH %s' % (table, file_format)
        fake_cur.copy_expert(copy_sql, output_file)

    fake_conn.commit()


@click.command(context_settings=dict(max_content_width=120))
@click.option('--dbname', '-d', help='database name to connect to')
@click.option('--host', '-h', help='database server host or socket directory (default: localhost)', default='localhost')
@click.option('--port', '-p', help='database server port (default: 5432)', default='5432')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use (default: public)')
@click.option('--password', '-W', hide_input=True, prompt=not found_config,
              default=config.DB_PASSWORD if found_config else None,
              help='database password (default is to prompt for password or read config)')
@click.option('--export', '-e', is_flag=True, help='export all tables to directory')
@click.option('--config', '-c', help='config file')
@click.argument('directory', default='tmp')
@click.version_option(version='0.0.1')
def main(dbname, host, port, username, password, schema,
         config, export, directory):

    url = "postgresql://%s:%s@%s:%s/%s" % (username, password, host, port, dbname)
    engine = create_engine(url)
    inspector = inspect(engine)
    if schema is None:
        schema = inspector.default_schema_name
        # print(inspector.get_schema_names())

    if export:
        export_all(engine, inspector, schema, directory)

if __name__ == "__main__":
    main()
