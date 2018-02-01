#!/usr/bin/env python3
import networkx as nx
# from sqlalchemy import create_engine, inspect


def get_simple_cycles(table_graph):
    return list(nx.simple_cycles(table_graph))


def get_fks_for_simple_cycles(table_graph, simple_cycles):
    fks = [table_graph.get_edge_data(cycle[0], cycle[1])['name'] for cycle in simple_cycles if len(cycle) == 2]
    fks.extend([table_graph.get_edge_data(cycle[1], cycle[0])['name'] for cycle in simple_cycles if len(cycle) == 2])
    return fks


def break_simple_cycles(table_graph):
    for cycle in nx.simple_cycles(table_graph):
        table_graph.remove_edge(cycle[0], cycle[-1])


def get_insertion_order(table_graph):
    copy_of_graph = table_graph.copy()
    break_simple_cycles(copy_of_graph)
    return nx.topological_sort(copy_of_graph, reverse=True)


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


def get_simple_cycle_fks_per_table(table_graph):
    simple_cycles = get_simple_cycles(table_graph)
    cycles = [cycle for cycle in simple_cycles if len(cycle) > 1]

    from collections import defaultdict
    cycle_fks_per_table = defaultdict(list)
    for table_a, table_b in cycles:
        cycle_fks_per_table[table_a].append(table_graph.get_edge_data(table_a, table_b)['name'])
        cycle_fks_per_table[table_b].append(table_graph.get_edge_data(table_b, table_a)['name'])

    return cycle_fks_per_table
