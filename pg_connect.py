import config
import db_meta
import psycopg2


def format_tuple(str_args):
    return str_args[0] % str_args[1]


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
    tables = db_meta.get_tables(cur, schema)

    print("Found %s tables in schema '%s'" % (len(tables), schema))
    for table in tables:
        print(table)
        # print(db_meta.get_column_names(cur, table, schema))
        # print_rows(cur, db_meta.sql_foreign_keys_of_table(table))

    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
