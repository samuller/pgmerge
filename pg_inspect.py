#!/usr/bin/env python3
import os
import click
import networkx as nx
from sqlalchemy import create_engine, inspect

found_config = True
try:
    import config
except ImportError:
    found_config = False


def print_missing_primary_keys(inspector, schema):
    no_pks = []
    for table in inspector.get_table_names(schema):
        pks = inspector.get_primary_keys(table, schema)
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
        print("\nDependency graph can be partitioned into %s sub-graphs:" % (len(sub_graphs),))
        for graph in sub_graphs:
            print(graph.nodes())


def print_insertion_order(table_graph):
    copy_of_graph = table_graph.copy()
    break_simple_cycles(copy_of_graph)
    print("\nInsertion order:")
    print(nx.topological_sort(copy_of_graph, reverse=True))


def build_fk_dependency_graph(inspector, schema, tables=None):
    table_graph = nx.DiGraph()
    if tables is None:
        tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        fks = inspector.get_foreign_keys(table, schema)
        table_graph.add_node(table)
        for fk in fks:
            assert fk['referred_schema'] == schema, 'Remote tables not supported'
            other_table = fk['referred_table']
            if other_table in tables:
                table_graph.add_edge(table, other_table, name=fk['name'])
    return table_graph


def graph_export_to_dot_file(table_graph, name='dependency_graph'):
    print('digraph %s {' % (name,))
    print('rankdir=LR; ranksep=1.0; size="16.5, 11.7";\n')
    for node in table_graph.nodes():
        for neighbour in table_graph[node]:
            edge = table_graph[node][neighbour].get('name')
            print('"%s" -> "%s" [label="%s"];' % (node, neighbour, edge))
    print('\n}')


def transferability(inspector, schema):
    operational = []
    knowledge = []
    transformable = []
    pk_contains_fk = []
    tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        columns = inspector.get_columns(table, schema)
        fks = inspector.get_foreign_keys(table, schema)
        pks = inspector.get_primary_keys(table, schema)
        uniques = inspector.get_unique_constraints(table, schema)

        for fk in fks:
            if not set(fk['constrained_columns']).isdisjoint(set(pks)):
                pk_contains_fk.append(table)

        auto_id = False
        default_columns = []
        for col in columns:
            if col['name'] in pks and col['default'] is not None:
                auto_id = True
            if col['default'] is not None:
                default_columns.append(col['name'])

        # For any unique constraint, if all columns don't have defaults, then we can use it
        auto_transformable = False
        for unique in uniques:
            if set(unique['column_names']).isdisjoint(set(default_columns)):
                auto_transformable = True

        if auto_id and not auto_transformable:
            operational.append(table)
        elif auto_id and auto_transformable:
            transformable.append(table)
        else:
            knowledge.append(table)

    print("\nOperational:\n", operational)
    print("\nKnowledge:\n", knowledge)
    print("\nAuto-transformable to knowledge:\n", transformable)
    print("\nPK contains FK:\n", pk_contains_fk)
    # graph_export_to_dot_file(build_fk_dependency_graph(inspector, schema, operational), name='operational')
    # graph_export_to_dot_file(build_fk_dependency_graph(inspector, schema, knowledge), name='knowledge')


@click.command(context_settings=dict(max_content_width=120))
@click.option('--dbname', '-d', help='database name to connect to')
@click.option('--host', '-h', help='database server host or socket directory', default='localhost')
@click.option('--port', '-p', help='database server port', default='5432')
@click.option('--username', '-U', help='database user name', default=lambda: os.environ.get('USER', 'postgres'))
@click.option('--schema', '-s', default="public", help='database schema to use (default: public)')
@click.option('--warnings', '-w', is_flag=True, help='Output any issues detected in database schema')
@click.option('--list-tables', '-t', is_flag=True, help="Output all tables found in the given schema")
@click.option('--table-details', '-td', is_flag=True,
              help="Output all tables along with column and foreign key information")
@click.option('--cycles', '-c', is_flag=True, help='Find and list cycles in foreign-key dependency graph')
@click.option('--insert-order', '-i', is_flag=True,
              help='Output the insertion order of tables based on the foreign-key dependency graph. ' +
              'This can be used by importer scripts if there are no circular dependency issues.')
@click.option('--partition', '-pt', is_flag=True,
              help='Partition and list sub-graphs of foreign-key dependency graph')
@click.option('--export-graph', '-e', is_flag=True,
              help='Output dot format description of foreign-key dependency graph.' +
                   ' To use graphviz to generate a PDF from this format, pipe the output to:' +
                   ' dot -Tpdf > graph.pdf')
@click.option('--transferable', '-x', is_flag=True, help='Output info related to table transfers')
# Either type password or avoid manual input with config file
@click.option('--password', '-W', hide_input=True, prompt=not found_config,
              default=config.DB_PASSWORD if found_config else None,
              help='database password (default is to prompt for password or read config)')
@click.version_option(version='0.0.1')
def main(dbname, host, port, username, password, schema,
         warnings, list_tables, table_details, partition,
         cycles, insert_order, export_graph, transferable):

    url = "postgresql://%s:%s@%s:%s/%s" % (username, password, host, port, dbname)
    engine = create_engine(url)
    inspector = inspect(engine)
    if schema is None:
        schema = inspector.default_schema_name
        # print(inspector.get_schema_names())

    if transferable:
        transferability(inspector, schema)
        return

    # Process database structure
    tables = sorted(inspector.get_table_names(schema))

    if list_tables:
        for table in tables:
            print(table)
    elif table_details:
        for table in tables:
            columns = inspector.get_columns(table, schema)
            fks = inspector.get_foreign_keys(table, schema)
            print("\ntable:", table)
            if len(columns) > 0:
                print("\tcolumns:", ", ".join([col['name'] for col in columns]))
            if len(fks) > 0:
                print("\tfks:", fks)
    elif not export_graph:
        print("Found %s tables in schema '%s'" % (len(tables), schema))

    if warnings:
        print_missing_primary_keys(inspector, schema)
        pass

    table_graph = nx.DiGraph()
    # Commands that require a graph to be generated
    if any([partition, cycles, insert_order, export_graph]):
        table_graph = build_fk_dependency_graph(inspector, schema)
    if partition:
        print_partition_info(table_graph)
    if cycles:
        print_cycle_info_and_break_cycles(table_graph)
    if insert_order:
        print_insertion_order(table_graph)
    if export_graph:
        graph_export_to_dot_file(table_graph)


if __name__ == "__main__":
    main()