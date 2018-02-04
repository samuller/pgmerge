#!/usr/bin/env python3
import networkx as nx
# from sqlalchemy import create_engine, inspect


def get_simple_cycles(graph):
    return list(nx.simple_cycles(graph))


def break_simple_cycles(graph):
    edges_removed = []
    for cycle in nx.simple_cycles(graph):
        graph.remove_edge(cycle[0], cycle[-1])
        edges_removed.append([cycle[0], cycle[-1]])
    return edges_removed


def convert_to_dag(directed_graph):
    """
    Convert graph to directed acyclic graph by breaking cycles.
    """
    edges_removed = break_simple_cycles(directed_graph)
    assert nx.is_directed_acyclic_graph(directed_graph), "Only simple cycles are currently supported."
    # cycle = nx.find_cycle(copy_of_graph)
    return edges_removed


def get_dependents(directed_acyclic_graph, node):
    assert nx.is_directed_acyclic_graph(directed_acyclic_graph), "Graph contains cycles."
    return nx.descendants(directed_acyclic_graph, node)


def get_fks_for_simple_cycles(table_graph, simple_cycles):
    fks = [table_graph.get_edge_data(cycle[0], cycle[1])['name'] for cycle in simple_cycles if len(cycle) == 2]
    fks.extend([table_graph.get_edge_data(cycle[1], cycle[0])['name'] for cycle in simple_cycles if len(cycle) == 2])
    return fks


def get_insertion_order(table_graph):
    copy_of_graph = table_graph.copy()
    convert_to_dag(copy_of_graph)
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
