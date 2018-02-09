

def get_unique_columns(inspector, table, schema):
    """
    If the combination of primary key and unique constraints is used to identify a row, then you'll miss rows where
    values in separate unique constraints have been swapped. This means that extra INSERTS or missed UPDATES could
    happen if these columns are collectively used as an identifier.
    """
    pks = inspector.get_primary_keys(table, schema)
    unique_constraints = inspector.get_unique_constraints(table, schema)
    unique = [col for constraint in unique_constraints for col in constraint['column_names']]
    return pks + unique


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


def pg_upsert(inspector, cursor, schema, dest_table, input_file, file_format="FORMAT CSV, HEADER"):
    """
    Postgresql 9.5+ includes merge/upsert with INSERT ... ON CONFLICT, but it requires columns to have unique
    constraints (or even a partial unique index). We might use it once we're sure that it covers all our use cases.
    """
    id_columns = get_unique_columns(inspector, dest_table, schema)
    if len(id_columns) == 0:
        return None

    all_columns = [col['name'] for col in inspector.get_columns(dest_table, schema)]
    stats = {'skip': 0, 'insert': 0, 'update': 0, 'total': 0}

    temp_table_name = "_tmp_%s" % (dest_table,)
    input_file = open(input_file, 'r')
    # Create temporary table with same columns and types as target table
    create_sql = "CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0;" % (temp_table_name, dest_table)
    cursor.execute(create_sql)
    # Import data into temporary table
    copy_sql = 'COPY %s FROM STDOUT WITH (%s)' % (temp_table_name, file_format)
    cursor.copy_expert(copy_sql, input_file)
    stats['total'] = cursor.rowcount

    # Delete rows in temp table that are already identical to those in destination table
    cursor.execute(sql_delete_identical_rows_between_tables(temp_table_name, dest_table, all_columns))
    stats['skip'] = cursor.rowcount

    # Insert rows from temp table that are not in destination table (according to id columns)
    cursor.execute(sql_insert_rows_not_in_table(dest_table, temp_table_name, id_columns))
    stats['insert'] = cursor.rowcount
    # Delete rows that were just inserted
    cursor.execute(sql_delete_identical_rows_between_tables(temp_table_name, dest_table, all_columns))

    # Update rows whose id columns match in destination table
    cursor.execute(sql_update_rows_between_tables(dest_table, temp_table_name, id_columns, all_columns))
    stats['update'] = cursor.rowcount

    drop_sql = "DROP TABLE %s" % (temp_table_name,)
    cursor.execute(drop_sql)

    # VACUUM is useful for each table that had major updates/import, but it has to run outside a transaction
    # and requires connection to be in autocommit mode
    # cursor.execute("VACUUM ANALYZE %s" % (dest_table,))

    return stats


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
    cursor.execute(sql)


def enable_foreign_key_constraints(cursor):
    sql = "SET session_replication_role = DEFAULT;"
    cursor.execute(sql)
