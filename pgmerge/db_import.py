"""
pgmerge - a PostgreSQL data import and merge utility

Copyright 2018 Simon Muller (samullers@gmail.com)
"""
import logging
from .db_export import *

_log = logging.getLogger(__name__)


def log_sql(sql):
    _log.debug('SQL: {}'.format(sql))


def exec_sql(cursor, sql):
    log_sql(sql)
    cursor.execute(sql)


def sql_delete_identical_rows_between_tables(delete_table_name, reference_table_name, all_column_names):
    # "IS NOT DISTINCT FROM" handles NULLS better (even composite type columns), but is not indexed
    # where_clause = " AND ".join(["%s.%s IS NOT DISTINCT FROM %s.%s" % (table, col, temp_table_name, col)
    #                               for col in all_columns])
    where_clause = " AND ".join(
        ["(%s.%s = %s.%s OR (%s.%s IS NULL AND %s.%s IS NULL))"
         % (reference_table_name, col, delete_table_name, col, reference_table_name, col, delete_table_name, col)
         for col in all_column_names])
    delete_sql = "DELETE FROM %s USING %s WHERE %s;" % \
                 (delete_table_name, reference_table_name, where_clause)
    return delete_sql


def sql_insert_rows_not_in_table(insert_table_name, reference_table_name, id_column_names):
    insert_table_cols = ",".join(["%s.%s" % (insert_table_name, col) for col in id_column_names])
    reference_table_cols = ",".join(["%s.%s" % (reference_table_name, col) for col in id_column_names])

    select_sql = "SELECT %s.* FROM %s LEFT JOIN %s ON (%s) = (%s) WHERE (%s) is NULL" %\
                 (reference_table_name, reference_table_name, insert_table_name,
                  insert_table_cols, reference_table_cols, insert_table_cols)

    insert_sql = "INSERT INTO %s (%s) RETURNING NULL;" % (insert_table_name, select_sql)
    return insert_sql


def sql_update_rows_between_tables(update_table_name, reference_table_name, id_column_names, all_column_names):
    # UPDATE table_b SET column1 = a.column1, column2 = a.column2, column3 = a.column3
    # FROM table_a WHERE table_a.id = table_b.id AND table_b.id in (1, 2, 3)
    set_columns = ",".join(["%s = %s.%s" % (col, reference_table_name, col)
                            for col in all_column_names])
    where_clause = " AND ".join(["%s.%s = %s.%s" % (update_table_name, col, reference_table_name, col)
                                 for col in id_column_names])
    update_sql = "UPDATE %s SET %s FROM %s WHERE %s" % \
                 (update_table_name, set_columns, reference_table_name, where_clause)
    return update_sql


def pg_upsert(inspector, cursor, schema, dest_table, input_file, file_format=None, columns=None, alternate_key=None):
    """
    Postgresql 9.5+ includes merge/upsert with INSERT ... ON CONFLICT, but it requires columns to have unique
    constraints (or even a partial unique index). We might use it once we're sure that it covers all our use cases.
    """
    if file_format is None:
        file_format = "FORMAT CSV, HEADER, ENCODING 'UTF8'"

    all_columns = [col['name'] for col in inspector.get_columns(dest_table, schema)]
    columns_sql = '*'
    if columns is not None:
        columns_sql = ','.join(columns)
    if columns is None:
        columns = all_columns

    id_columns = get_unique_columns(inspector, dest_table, schema)
    if len(id_columns) == 0:
        raise UnsupportedSchemaException("Table has no primary key or unique columns!")

    unknown_columns = set(columns) - set(all_columns)
    if len(unknown_columns) > 0:
        raise InputParametersException("Columns provided do not exist in table '{}': {}"
                                       .format(dest_table, unknown_columns))

    skipped_id_columns = set(id_columns) - set(columns)
    if len(skipped_id_columns) > 0:
        raise InputParametersException("Columns provided do not include required id"
                              " columns for table '{}': {}".format(dest_table, skipped_id_columns))

    stats = {'skip': 0, 'insert': 0, 'update': 0, 'total': 0}

    table_name_tmp_copy = "_tmp_copy_%s" % (dest_table,)

    foreign_columns = [(col, []) for col in columns]
    select_sql = sql_select_table_with_foreign_columns(inspector, schema, dest_table, foreign_columns,
                                                       alias_columns=False)
    # Create temporary table with same columns and types as target table
    create_sql = "CREATE TEMP TABLE {} AS {select_sql} LIMIT 0;".format(table_name_tmp_copy, select_sql=select_sql)
    exec_sql(cursor, create_sql)
    # Import data into temporary table
    copy_sql = 'COPY %s FROM STDOUT WITH (%s)' % (table_name_tmp_copy, file_format)
    log_sql(copy_sql)

    with open(input_file, 'r', encoding="utf-8") as input_file:
        cursor.copy_expert(copy_sql, input_file)
    stats['total'] = cursor.rowcount

    # select_sql = sql_select_table_with_foreign_columns(inspector, schema, dest_table)
    table_name_tmp_final = "_tmp_final_%s" % (dest_table,)
    select_sql = sql_select_table_with_local_columns(inspector, schema, dest_table,
                                                     table_name_tmp_copy, foreign_columns)
    create_sql = "CREATE TEMP TABLE {} AS {select_sql};".format(
        table_name_tmp_final, select_sql=select_sql)
    exec_sql(cursor, create_sql)

    upsert_stats = upsert_table_to_table(cursor, table_name_tmp_final, dest_table, id_columns, columns)
    stats.update(upsert_stats)

    drop_sql = "DROP TABLE %s" % (table_name_tmp_copy,)
    exec_sql(cursor, drop_sql)

    drop_sql = "DROP TABLE %s" % (table_name_tmp_final,)
    exec_sql(cursor, drop_sql)

    return stats


def upsert_table_to_table(cursor, src_table, dest_table, id_columns, columns):
    stats = {'skip': 0, 'insert': 0, 'update': 0}

    # Delete rows in temp table that are already identical to those in destination table
    exec_sql(cursor, sql_delete_identical_rows_between_tables(src_table, dest_table, columns))
    stats['skip'] = cursor.rowcount

    # Insert rows from temp table that are not in destination table (according to id columns)
    exec_sql(cursor, sql_insert_rows_not_in_table(dest_table, src_table, id_columns))
    stats['insert'] = cursor.rowcount
    # Delete rows that were just inserted
    exec_sql(cursor, sql_delete_identical_rows_between_tables(src_table, dest_table, columns))

    # Update rows whose id columns match in destination table
    exec_sql(cursor, sql_update_rows_between_tables(dest_table, src_table, id_columns, columns))
    stats['update'] = cursor.rowcount

    return stats


def sql_select_table_with_local_columns(inspector, schema, schema_table, src_table,
                                        foreign_columns, local_columns_subset=None):
    """
    :param schema_table: Has foreign keys
    :param src_table: Will be selected from
    """
     # Check correctness of paths and build up all foreign keys possibly needed
    all_fks = inspector.get_foreign_keys(schema_table, schema)
    fks_by_name = {fk['name']: fk for fk in all_fks}

    grouped_foreign_columns = {tuple(path): path for _, path in foreign_columns}
    paths = list(grouped_foreign_columns.keys())
    paths.sort(key=lambda path: len(path))
    for path in paths:
        if len(path) == 0:
            continue
        if path[-1] not in fks_by_name:
            # To be able to join to path [fk1, fk2, fk3] we also need path [fk1, fk2] somewhere
            raise InputParametersException("Partial path missing for: {}".format(path))
        final_fk = fks_by_name[path[-1]]
        new_fks = inspector.get_foreign_keys(final_fk['referred_table'], schema)
        fks_by_name.update({fk['name']: fk for fk in new_fks})
    # Go through all foreign columns and collect all 'replaced columns'
    for path in paths:
        if len(path) == 0:
            continue
        foreign_column = grouped_foreign_columns[path]
        final_fk = fks_by_name[path[-1]]
        final_fk.setdefault('replaced_columns', []).append(foreign_column[0])
    # Create joins for all foreign keys
    per_join_sql = []
    for path in paths:
        if len(path) == 0:
            continue
        elif len(path) == 1:
            cur_table = src_table
        else:
            cur_table = sql_join_alias_for_foreign_key(path[-2])

        final_fk = fks_by_name[path[-1]]
        # TODO: consider if join using only last reference can work when foreign key path is known
        per_join_sql.append(sql_join_from_foreign_key(final_fk, cur_table,
                                                      local_columns_key='replaced_columns',
                                                      foreign_columns_key='replaced_columns'))

    joins_sql = " " + " ".join(per_join_sql)
    # TODO: add non-local columns
    columns_sql = ','.join(["{}".format(col) for col, path in foreign_columns if len(path) == 0])

    # We don't use {schema}.{src_table} since that doesn't allow temporary tables
    return "SELECT {columns_sql} FROM {src_table}{joins_sql};".format(
        columns_sql=columns_sql, src_table=src_table, joins_sql=joins_sql)


def sql_join_alias_for_foreign_key(foreign_key_name):
    return 'join_{}'.format(foreign_key_name)


def disable_foreign_key_constraints(cursor):
    """
    There are different possible approaches for disabling foreign keys. The following are some options that
    disable and re-enable foreign keys globally [1]:
        SET session_replication_role = REPLICA; -- [2]
        SET session_replication_role = DEFAULT;
    or
        SET CONSTRAINTS ALL DEFERRED;
        SET CONSTRAINTS ALL IMMEDIATE;

    Options for disabling and re-enabling foreign keys per table are [3]:
        ALTER TABLE table_name DISABLE TRIGGER ALL;
        ALTER TABLE table_name ENABLE TRIGGER ALL;
    or
        ALTER TABLE table_name ALTER CONSTRAINT table_fkey DEFERRABLE; -- [4]
        SET CONSTRAINTS table_fkey DEFERRED;
    or
        ALTER TABLE table_name DROP CONSTRAINT table_fkey;
        ALTER TABLE table_name ADD CONSTRAINT table_fkey FOREIGN KEY (column_name)
            REFERENCES other_table_name (other_column_name) ON DELETE RESTRICT;
    The second and third options might need extra code to leave constraints in the same state as before (e.g.
    NOT DEFERRABLE or ON UPDATE CASCADE).

    Some of these options work on most constraints, but not foreign keys. Most cases where foreign keys can
    be disabled, it seems to require superuser rights.

    [1][https://stackoverflow.com/questions/3942258/how-do-i-temporarily-disable-triggers-in-postgresql]
    [2][https://www.postgresql.org/docs/current/static/runtime-config-client.html]
    [3][https://stackoverflow.com/questions/38112379/disable-postgresql-foreign-key-checks-for-migrations]
    [4][https://www.postgresql.org/docs/current/static/sql-altertable.html]
    """
    sql = "SET session_replication_role = REPLICA;"
    exec_sql(cursor, sql)


def enable_foreign_key_constraints(cursor):
    sql = "SET session_replication_role = DEFAULT;"
    exec_sql(cursor, sql)


class PreImportException(Exception):
    """
    Exception raised for errors detected before starting import.
    """

    def __init__(self, message):
        super().__init__(message)


class UnsupportedSchemaException(PreImportException):
    """
    Exception raised due to database schema being unsupported by import.
    """

    def __init__(self, message):
        super().__init__(message)


class InputParametersException(PreImportException):
    """
    Exception raised due to incorrect parameters provided to import.
    """

    def __init__(self, message):
        super().__init__(message)
