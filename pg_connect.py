import config
import psycopg2
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


def sql_tables_in_db(schema="public"):
    """Generate sql query to fetch all tables"""
    sql = ("SELECT table_name FROM information_schema.tables" +
           " WHERE table_schema = '%s'" +
           " ORDER BY table_schema,table_name;") % (
        schema)
    return sql


def sql_foreign_keys_of_table(table, schema="public"):
    """
    Does not work correctly for foreign key constraints that point
    to multiple columns
    """
    sql = """SELECT
        tc.constraint_name, tc.table_name, kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';""" % \
          (table,)
    return sql


def psql_foreign_keys_of_table(table, schema="public"):
    """
    Postgres-specific and gives constraint's definition in sql
    """
    sql = """SELECT conname,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef
    FROM pg_catalog.pg_constraint r
    WHERE r.conrelid = '%s.%s'::regclass AND r.contype = 'f'
    ORDER BY conname;""" % \
          (schema, table,)
    return sql


def take_first(list_of_tuples):
    return [tup[0] for tup in list_of_tuples]


def get_tables(cursor, schema="public"):
    sql = sql_tables_in_db(schema)
    print(sql)

    cursor.execute(sql)
    return take_first(cursor.fetchall())


def get_column_names(cursor, table, schema="public"):
    sql = "SELECT * FROM %s.%s LIMIT 0" % (schema, table)
    cursor.execute(sql)
    return take_first(cursor.description)


def print_rows(cursor, sql):
    cursor.execute(sql)
    count = cursor.rowcount
    # rows = list(cursor)
    print("%s results" % count)
    for row in cursor:
        print(row)
    return count


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
        # print_rows(cur, sql_foreign_keys_of_table(table))

    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
