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
    tables = db_meta.get_table_names(cur, schema)

    print("Found %s tables in schema '%s'" % (len(tables), schema))
    no_pks = []
    for table in tables:
        # print("\n", table)
        # print(db_meta.get_columns(cur, table, schema))
        # print(db_meta.get_foreign_keys(cur, table))
        pks = db_meta.get_primary_key_column_names(cur, table)
        if len(pks) == 0:
            no_pks.append(table)
    if len(no_pks) > 0:
        print("The following tables have no primary key: ", no_pks)

    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
