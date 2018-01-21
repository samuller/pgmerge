#
# We might consider constructing our queries with the PyPika library in the future,
# but most of the main reasons it was considered are currently covered:
# - queries that aren't db-specific:
#   - not currently a priority
# - dynamically generating sql WHERE clauses to make filtering parameters optional:
#   - we use the following sql pattern "WHERE value is null or column = value"
# - escaping strings:
#  - done by psycopg2 when using parameters
# - avoiding sql injections:
#  - current use cases shouldn't require this
#  - using psycopg2 parameters does this for us
#  - Pypika doesn't seem to be able to use parameters and it's protection against sql injections is unknown
#
# from pypika import PostgreSQLQuery as Query, Table, Field
from psycopg2 import sql as psql


class TableColumn:

    def __init__(self, table_name, column_name, primary_constraint=None, unique_constraints=None):
        self.table_name = table_name
        self.column_name = column_name
        self.primary_constraint = primary_constraint
        self.unique_constraints = unique_constraints

        if unique_constraints is None:
            self.unique_constraints = []

    def __repr__(self):
        return "%s.%s%s" % (self.table_name, self.column_name,
                            " *" if self.primary_constraint is not None else "")


class ForeignKey:

    def __init__(self, name, table, columns, other_table, other_columns):
        self.name = name
        self.table = table
        self.columns = columns
        self.other_table = other_table
        self.other_columns = other_columns

    def __repr__(self):
        return "%s: %s.%s -> %s.%s" % (self.name, self.table, self.columns, self.other_table, self.other_columns)


def get_table_names(cursor, schema="public"):
    cursor.execute(sql_tables_in_db(), {'schema': schema})
    return [row[1] for row in cursor]


def get_column_names(cursor, table, schema="public"):
    cursor.execute(psql.SQL("SELECT * FROM {}.{} LIMIT 0")
                   .format(psql.Identifier(schema), psql.Identifier(table)))
    return [row[0] for row in cursor.description]


def get_primary_key_column_names(cursor, table, schema="public"):
    cursor.execute(sql_primary_keys(), {'schema': schema, 'table': table})
    return [row[2] for row in cursor]


def get_unique_column_names(cursor, table, schema="public"):
    cursor.execute(sql_unique_columns(), {'schema': schema, 'table': table})
    return [row[2] for row in cursor]


def get_primary_keys_by_column_name(cursor, table, schema="public"):
    return get_constraints_by_column_name(cursor, sql_primary_keys(), table, schema)


def get_unique_constraints_by_column_name(cursor, table, schema="public"):
    return get_constraints_by_column_name(cursor, sql_unique_columns(), table, schema)


def get_constraints_by_column_name(cursor, sql, table, schema="public"):
    assert schema is not None
    assert table is not None
    cursor.execute(sql, {'schema': schema, 'table': table})
    col_to_constraint = {}
    for row in cursor:
        name = row[3]
        col_name = row[2]
        col_to_constraint.setdefault(col_name, []).append(name)
    return col_to_constraint


def get_columns(cursor, table, schema="public"):
    pks = get_primary_keys_by_column_name(cursor, table, schema)
    uniques = get_unique_constraints_by_column_name(cursor, table, schema)
    return [TableColumn(table, col, pks.get(col, None), uniques.get(col, []))
            for col in get_column_names(cursor, table, schema)]


def get_foreign_keys(cursor, table=None, schema="public"):
    cursor.execute(sql_foreign_keys_of_table(), {'schema': schema, 'table': table})
    return [ForeignKey(row[2], row[0], [row[1]], row[3], [row[4]])
            for row in cursor]


def sql_tables_in_db():
    """Generate sql query to fetch all tables"""
    return """
    SELECT table_schema, table_name FROM information_schema.tables
    WHERE (%(schema)s is null OR table_schema = %(schema)s)
    ORDER BY table_schema, table_name;
    """


def sql_foreign_keys_of_table():
    """
    Does not work correctly for foreign key constraints that point
    to multiple columns. Also, duplicates are possible as constraint names
    aren't necessarily unique in Postgres (see note at bottom of:
    https://www.postgresql.org/docs/10/static/information-schema.html )
    """
    return """
    SELECT
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
        AND (%(schema)s is null OR tc.table_schema = %(schema)s)
        AND (%(table)s is null OR tc.table_name = %(table)s)
    ORDER BY tc.constraint_name;"""


def psql_foreign_keys_of_table():
    """
    Postgres-specific version which is more accurate but harder to interpret as
    it gives constraint's definition in sql syntax.

    schema_table = schema + "." + table
    (schema_table, schema_table)
    """
    return """
    SELECT
        conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
    FROM pg_catalog.pg_constraint r
    WHERE r.contype = 'f'
        AND (%s is null OR r.conrelid = %s::regclass)
    ORDER BY conname;"""


def sql_primary_keys():
    return """SELECT table_schema, table_name, column_name, constraint_name, ordinal_position
    FROM information_schema.table_constraints
    JOIN information_schema.key_column_usage
    USING (constraint_catalog, constraint_schema, constraint_name,
          table_catalog, table_schema, table_name)
    WHERE constraint_type = 'PRIMARY KEY'
        AND (%(schema)s is null OR table_schema = %(schema)s)
        AND (%(table)s is null OR table_name = %(table)s)
    ORDER BY ordinal_position;"""

# Unique constraint types:
# - column-constraint
# - table-constraint
# - unique partial index
# PRIMARY_KEY constraint_type also indicates uniqueness (UNIQUE NOT NULL)
def sql_unique_columns():
    return """SELECT table_schema, table_name, column_name, constraint_name, ordinal_position
    FROM information_schema.table_constraints
    JOIN information_schema.key_column_usage
    USING(constraint_catalog, constraint_schema, constraint_name,
          table_catalog, table_schema, table_name)
    WHERE constraint_type in ('UNIQUE')
        AND (%(schema)s is null OR table_schema = %(schema)s)
        AND (%(table)s is null OR table_name = %(table)s)
    ORDER BY ordinal_position;"""


def sql_column_default_values():
    return """SELECT column_name, column_default
    FROM information_schema.columns
    WHERE (%(schema)s is null OR table_schema = %(schema)s)
        AND (%(table)s is null OR table_name = %(table)s)
    ORDER BY ordinal_position;
    """
