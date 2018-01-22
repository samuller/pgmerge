import config
import db_meta
import click
import psycopg2
import networkx as nx
import os
import getpass


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


@click.command()
@click.option('--dbname', '-d', help='database name to connect to')
@click.option('--host', '-h', help='database server host or socket directory')
@click.option('--port', '-p', help='database server port')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--verbose', '-v', is_flag=True)
# @click.option('--password', '-W', help='database user name', prompt=True, hide_input=True)
@click.version_option(version='0.0.1')
def main(dbname, host, port, username, verbose):
    if config.DB_PASSWORD is None:
        password = getpass.getpass()
    else:
        password = config.DB_PASSWORD

    # Connect to an existing database
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=username, password=password)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Query the database and obtain data as Python objects

    # Process database structure
    schema = "public"
    tables = db_meta.get_table_names(cur, schema)

    no_pks = []
    table_graph = nx.DiGraph()
    for table in tables:
        if verbose:
            print("\n", table)
            print(db_meta.get_columns(cur, table, schema))
            print(db_meta.get_foreign_keys(cur, table))

        pks = db_meta.get_primary_key_column_names(cur, table)
        if len(pks) == 0:
            no_pks.append(table)

        fks = db_meta.get_foreign_keys(cur, table)
        table_graph.add_node(table)
        for fk in fks:
            table_graph.add_edge(fk.table, fk.other_table)

    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()

    # Print warnings and info
    print("Found %s tables in schema '%s'" % (len(tables), schema))
    print(tables)
    if len(no_pks) > 0:
        print("\n%s tables have no primary key:" % (len(no_pks),))
        print(no_pks)

    print_partition_info(table_graph)
    print_cycle_info_and_break_cycles(table_graph)
    print_insertion_order(table_graph)


if __name__ == "__main__":
    main()
