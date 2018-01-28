#!/usr/bin/env python3
import networkx as nx
# from sqlalchemy import create_engine, inspect


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
