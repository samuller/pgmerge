import config
import db_meta
import click
import psycopg2
import networkx as nx
import os
import getpass


def print_missing_primary_keys(cursor, tables):
    no_pks = []
    for table in tables:
        pks = db_meta.get_primary_key_column_names(cursor, table)
        if len(pks) == 0:
            no_pks.append(table)
    if len(no_pks) > 0:
        print("\n%s tables have no primary key:" % (len(no_pks),))
        print(no_pks)


def break_simple_cycles(table_graph):
    for cycle in nx.simple_cycles(table_graph):
        table_graph.remove_edge(cycle[0], cycle[-1])


def print_cycle_info_and_break_cycles(table_graph):
    """
    Changes given graph by breaking cycles
    """
    simple_cycles = list(nx.simple_cycles(table_graph))
    if len(simple_cycles) > 0:
        print("\n%s self-references and simple cycles found:" % (len(simple_cycles),))
        print(simple_cycles)

    # Break simple cycles and self-references to help find bigger cycles
    copy_of_graph = table_graph.copy()
    break_simple_cycles(copy_of_graph)

    try:
        cycle = nx.find_cycle(copy_of_graph)
        print("\nAnother cycle was detected:")
        print(cycle)
    except nx.exception.NetworkXNoCycle:
        pass


def print_partition_info(table_graph):
    sub_graphs = [graph for graph in nx.weakly_connected_component_subgraphs(table_graph)]
    if len(sub_graphs) > 1:
        print("\nReferences can be partitioned into %s sub-graphs:" % (len(sub_graphs),))
        for graph in sub_graphs:
            print(graph.nodes())


def print_insertion_order(table_graph):
    copy_of_graph = table_graph.copy()
    break_simple_cycles(copy_of_graph)
    print("\nInsertion order:")
    print(nx.topological_sort(copy_of_graph, reverse=True))


def build_fk_dependency_graph(cursor, tables):
    table_graph = nx.DiGraph()
    for table in tables:
        fks = db_meta.get_foreign_keys(cursor, table)
        table_graph.add_node(table)
        for fk in fks:
            table_graph.add_edge(fk.table, fk.other_table)
    return table_graph


@click.command()
@click.option('--dbname', '-d', help='database name to connect to')
@click.option('--host', '-h', help='database server host or socket directory')
@click.option('--port', '-p', help='database server port')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use (default: public)')
@click.option('--verbose', '-v', is_flag=True)
@click.option('--warnings', '-w', is_flag=True, help='Output any issues detected in database schema')
@click.option('--list-tables', '-t', is_flag=True)
@click.option('--cycles', '-c', is_flag=True, help='Find and list cycles in foreign-key dependency graph')
@click.option('--insert-order', '-i', is_flag=True,
              help='Output the insertion order of tables based on the foreign-key dependency graph. ' +
              'This can be used by importer scripts if there are no circular dependency issues.')
@click.option('--partition', '-pt', is_flag=True,
              help='Partition and list sub-graphs of foreign-key dependency graph')
# @click.option('--password', '-W', help='database user name', prompt=True, hide_input=True)
@click.version_option(version='0.0.1')
def main(dbname, host, port, username, schema, verbose, warnings, list_tables, partition, cycles, insert_order):
    # Either type password or avoid manual input with config file
    if config.DB_PASSWORD is None:
        password = getpass.getpass()
    else:
        password = config.DB_PASSWORD

    # Connect to an existing database and open a cursor to perform database operations
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=username, password=password)
    cursor = conn.cursor()

    # Process database structure
    tables = db_meta.get_table_names(cursor, schema)
    print("Found %s tables in schema '%s'" % (len(tables), schema))

    for table in tables:
        if verbose:
            print("\n", table)
            print(db_meta.get_columns(cursor, table, schema))
            print(db_meta.get_foreign_keys(cursor, table))

    if list_tables:
        print(tables)
    if warnings:
        print_missing_primary_keys(cursor, tables)

    table_graph = nx.DiGraph()
    if partition or cycles or insert_order:
        table_graph = build_fk_dependency_graph(cursor, tables)
    if partition:
        print_partition_info(table_graph)
    if cycles:
        print_cycle_info_and_break_cycles(table_graph)
    if insert_order:
        print_insertion_order(table_graph)


    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
