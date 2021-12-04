#!/usr/bin/env python3
"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import logging
import networkx as nx
from typing import Any, List, Dict, Set, Optional, cast

_log = logging.getLogger(__name__)


def get_cycles(graph: Any) -> List[Any]:
    """Find cycles in the given graph."""
    return list(nx.simple_cycles(graph))


def break_cycles(graph: Any) -> List[Any]:
    """Remove edges to break cycles found in the given graph."""
    edges_removed = []
    simple_cycles = list(nx.simple_cycles(graph))
    # Ensure the cycles are sorted
    for cycle in simple_cycles:
        cycle.sort()
    simple_cycles.sort()
    for cycle in simple_cycles:
        # Only remove one direction of dependency and also remove self-references
        graph.remove_edge(cycle[0], cycle[-1])
        edges_removed.append([cycle[0], cycle[-1]])
    return edges_removed


def convert_to_dag(directed_graph: Any) -> List[Any]:
    """Convert graph to directed acyclic graph by breaking cycles."""
    edges_removed = break_cycles(directed_graph)
    return edges_removed


def get_dependents(directed_acyclic_graph: Any, node: Any) -> List[Any]:
    """Get all dependent/descendant nodes for a given node."""
    assert nx.is_directed_acyclic_graph(directed_acyclic_graph), "Graph contains cycles."
    return list(nx.descendants(directed_acyclic_graph, node))


def get_fks_for_direct_cycles(table_graph: Any, direct_cycles: List[Any]) -> List[str]:
    """Get the corresponding foreign key names for the given cycles."""
    fks = [table_graph.get_edge_data(cycle[0], cycle[1])['name'] for cycle in direct_cycles if len(cycle) == 2]
    fks.extend([table_graph.get_edge_data(cycle[1], cycle[0])['name'] for cycle in direct_cycles if len(cycle) == 2])
    return fks


def get_insertion_order(table_graph: Any) -> List[Any]:
    """Determine a valid insertion order so that tables that are depended-on are inserted first."""
    copy_of_graph = table_graph.copy()
    convert_to_dag(copy_of_graph)
    return list(reversed(list(nx.topological_sort(copy_of_graph))))


def build_fk_dependency_graph(inspector: Any, schema: str, tables: Optional[List[str]] = None) -> Any:
    """Build a dependency graph of based on the foreign keys and tables in the database schema."""
    table_graph = nx.OrderedDiGraph()
    if tables is None:
        tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        fks = inspector.get_foreign_keys(table, schema)
        table_graph.add_node(table)
        for fky in fks:
            assert fky['referred_schema'] == schema, 'Remote tables not supported'
            other_table = fky['referred_table']
            if other_table in tables:
                table_graph.add_edge(table, other_table, name=fky['name'])
    return table_graph


def get_direct_cycle_fks_per_table(table_graph: Any) -> Dict[str, List[str]]:  # pragma: no cover
    """Search through tables for "direct cycles" where a pair of tables depend on one another."""
    cycles = get_cycles(table_graph)
    cycles = [cycle for cycle in cycles if len(cycle) > 1]

    from collections import defaultdict
    cycle_fks_per_table = defaultdict(list)
    for table_a, table_b in cycles:
        cycle_fks_per_table[table_a].append(table_graph.get_edge_data(table_a, table_b)['name'])
        cycle_fks_per_table[table_b].append(table_graph.get_edge_data(table_b, table_a)['name'])

    return cycle_fks_per_table


def get_all_dependent_tables(table_graph: Any, tables: List[str]) -> Set[str]:
    """
    Find all the tables on which the given set of tables depends.

    I.e. if the table has a foreign key dependency on a table and that table has a dependency
    on 2 other tables, then we'll get all 3 tables. We return all referenced tables as well as
    the given set of tables.
    """
    dependent_tables = set()
    trees = []
    for table in sorted(tables):
        dependency_tree = nx.dfs_successors(table_graph, table)
        dependent_tables.add(table)  # Not in tree if it doesn't have any dependents
        dependent_tables.update(set(dependency_tree.keys()))
        dependent_tables.update({node for dependents in dependency_tree.values() for node in dependents})
        trees.append((table, dependency_tree))

    if len(dependent_tables) > len(set(tables)):
        print('Also including the following dependent tables:\n')
        for table, dependency_tree in trees:
            for node in sorted(dependency_tree.keys(), key=lambda x: cast(str, '' if x == table else x)):
                indent = '\t' if node == table else '\t  '
                print(indent + '{} -> {}'.format(node, ', '.join(sorted(dependency_tree[node]))))
        print('')
        print('Final tables exported: ' + ' '.join(sorted(list(dependent_tables))))
        print('')

    return dependent_tables
