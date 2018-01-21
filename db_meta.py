# TODO:
# - use sql queries with parameters
# - allow generating queries that don't filter on schema or table
from pypika import PostgreSQLQuery as Query, Table, Field


class TableColumn:

    def __init__(self, table_name, column_name, is_primary_key=None):
        self.table_name = table_name
        self.column_name = column_name
        self.is_primary_key = is_primary_key

    def __repr__(self):
        return "%s.%s%s" % (self.table_name, self.column_name, " *" if self.is_primary_key else "")


class ForeignKey:

    def __init__(self, name, table, columns, other_table, other_columns):
        self.name = name
        self.table = table
        self.columns = columns
        self.other_table = other_table
        self.other_columns = other_columns

    def __repr__(self):
        return "%s: %s.%s -> %s.%s" % (self.name, self.table, self.columns, self.other_table, self.other_columns)


def take_first(list_of_tuples):
    return [tup[0] for tup in list_of_tuples]


def get_table_names(cursor, schema="public"):
    sql = sql_tables_in_db(schema)
    cursor.execute(sql)
    return take_first(cursor.fetchall())


def get_column_names(cursor, table, schema="public"):
    sql = "SELECT * FROM %s.%s LIMIT 0" % (schema, table)
    cursor.execute(sql)
    return take_first(cursor.description)


def get_primary_key_column_names(cursor, table, schema="public"):
    sql = sql_primary_keys(table, schema)
    cursor.execute(sql)
    return take_first(cursor.fetchall())


def get_columns(cursor, table, schema="public"):
    pk_columns = get_primary_key_column_names(cursor, table, schema)
    return [TableColumn(table, col, col in pk_columns) for col in get_column_names(cursor, table, schema)]


def get_foreign_keys(cursor, table, schema="public"):
    cursor.execute(sql_foreign_keys_of_table(table, schema))
    return [ForeignKey(row[2], row[0], [row[1]], row[3], [row[4]])
            for row in cursor]


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
        tc.table_name, kcu.column_name, tc.constraint_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = '%s'
        AND tc.table_name='%s'
    ORDER BY tc.constraint_name;""" % \
          (schema, table)
    return sql


def psql_foreign_keys_of_table(table, schema="public"):
    """
    Postgres-specific is harder to interpret as it gives
    constraint's definition in sql syntax
    """
    sql = """SELECT conname,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef
    FROM pg_catalog.pg_constraint r
    WHERE r.conrelid = '%s.%s'::regclass
        AND r.contype = 'f'
    ORDER BY conname;""" % \
          (schema, table,)
    return sql


def sql_primary_keys(table, schema="public"):
    sql = """SELECT column_name
    FROM information_schema.table_constraints
    JOIN information_schema.key_column_usage
    USING(constraint_catalog, constraint_schema, constraint_name,
          table_catalog, table_schema, table_name)
    WHERE constraint_type = 'PRIMARY KEY'
        AND table_schema = '%s'
        AND table_name = '%s'
    ORDER BY ordinal_position;""" % (schema, table)
    return sql


def sql_column_default_values(table, schema=""):
    sql = """SELECT column_name, column_default
    FROM information_schema.columns
    WHERE table_schema = '%s'
        AND table_name = '%s'
    ORDER BY ordinal_position;
    """ % (schema, table)
    return sql

