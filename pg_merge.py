#!/usr/bin/env python3
import os
import click
from sqlalchemy import create_engine, inspect

found_config = True
try:
    import config as cfg
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


def get_unique_columns(inspector, table, schema):
    pks = inspector.get_primary_keys(table, schema)
    unique_constraints = inspector.get_unique_constraints(table, schema)
    unique = [col for constraint in unique_constraints for col in constraint['column_names']]
    return pks + unique


def import_all(engine, inspector, schema, input_dir, file_format="CSV HEADER"):
    fake_conn = engine.raw_connection()
    fake_cur = fake_conn.cursor()
    # assert fake_conn.server_version >= 90500, \
    #     'Postgresql 9.5 or later required for INSERT ... ON CONFLICT: %s' % (fake_conn.server_version,)

    tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        id_columns = get_unique_columns(inspector, table, schema)
        if len(id_columns) == 0:
            print("Skipping table '%s' as it has no primary key or unique columns!" % (table,))
            continue
        temp_table_name = "_tmp_%s" % (table,)
        input_file = open(os.path.join(input_dir, table + '.csv'), 'r')
        # Create temporary table with same columns and types as target table
        create_sql = "CREATE TEMP TABLE %s AS SELECT * from %s LIMIT 0;" % (temp_table_name, table)
        fake_cur.execute(create_sql)

        copy_sql = 'COPY %s FROM STDOUT WITH %s' % (temp_table_name, file_format)
        fake_cur.copy_expert(copy_sql, input_file)

        fake_cur.execute("SELECT count(*) from %s;" % (temp_table_name,))
        # print("%s: %s" % (table, fake_cur.fetchone()[0]))


        drop_sql = "DROP TABLE %s" % (temp_table_name,)
        fake_cur.execute(drop_sql)

    fake_conn.commit()


@click.command(context_settings=dict(max_content_width=120))
@click.option('--dbname', '-d', help='database name to connect to')
@click.option('--host', '-h', help='database server host or socket directory (default: localhost)', default='localhost')
@click.option('--port', '-p', help='database server port (default: 5432)', default='5432')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use (default: public)')
@click.option('--password', '-W', hide_input=True, prompt=not found_config,
              default=cfg.DB_PASSWORD if found_config else None,
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
    else:
        print("TODO: implement import")
        # import_all(engine, inspector, schema, directory)


if __name__ == "__main__":
    main()
