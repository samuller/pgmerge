import config
import psycopg2
# python-sql
import sql as ps
import sql.aggregate as psa
import sql.conditionals as psc
# pypika
from pypika import PostgreSQLQuery as Query, Table, Field


def format_tuple(str_args):
    return str_args[0] % str_args[1]


def pypika_get_tables(schema="public"):
    """Use PyPika to generate sql query to fetch all tables"""
    tables = Table('tables', schema='information_schema')
    q = Query.from_(tables).select(
        'table_name'
    ).where(
        tables.table_schema == schema
    ).orderby(
        tables.table_schema
    ).orderby(
        'table_name'
    )
    sql = q.get_sql(quote_char=None)
    return sql


def python_sql_get_tables(schema="public"):
    """Use python-sql to generate sql query to fetch all tables"""
    tables = ps.Table('information_schema.tables')
    select = tables.select()
    select.where = tables.table_schema == schema
    select.order_by = (tables.table_schema, tables.table_name)
    print(tuple(select))
    print(format_tuple(tuple(select)))
    sql = format_tuple(tuple(select))
    return sql


def sql_get_tables(schema="public"):
    """Generate sql query to fetch all tables"""
    sql = ("SELECT table_name FROM information_schema.tables" +
           " WHERE table_schema = '%s'" +
           " ORDER BY table_schema,table_name;") % (
        schema)
    return sql


def get_tables(cursor, schema="public"):
    sql = sql_get_tables(schema)
    print(sql)

    cursor.execute(sql)
    tables = [tbl[0] for tbl in cursor.fetchall()]
    return tables


def get_column_names(cursor, table, schema="public"):
    sql = "SELECT * FROM %s.%s LIMIT 0" % (schema, table)
    cursor.execute(sql)
    col_names = [desc[0] for desc in cursor.description]
    return col_names


def main():
    # Connect to an existing database
    conn = psycopg2.connect(
        host=config.DB_HOST,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Query the database and obtain data as Python objects
    schema = "public"
    tables = get_tables(cur, schema)

    print("Found %s tables in schema '%s'" % (len(tables), schema))
    for table in tables:
        print(table)
        # print(get_column_names(cur, table, schema))

    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
