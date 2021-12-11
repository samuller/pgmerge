#!/usr/bin/env python3
"""
pgmerge - a PostgreSQL data import and merge utility.

Copyright 2018-2021 Simon Muller (samullers@gmail.com)
"""
import logging

import networkx as nx
from sqlalchemy import inspect
from typing import Any, List, Optional

from . import db_graph

_log = logging.getLogger(__name__)


def print_missing_primary_keys(inspector: Any, schema: str) -> None:
    """Find and print tables in database that don't have primary keys."""
    no_pks = []
    for table in inspector.get_table_names(schema):
        pks = inspector.get_pk_constraint(table, schema)
        if len(pks) == 0:
            no_pks.append(table)
    if len(no_pks) > 0:
        print("\n%s tables have no primary key:" % (len(no_pks),))
        print(no_pks)


def print_cycle_info_and_break_cycles(table_graph: Any) -> None:
    """Change given graph by breaking cycles."""
    simple_cycles = list(nx.simple_cycles(table_graph))
    if len(simple_cycles) > 0:
        print("\n%s self-references and simple cycles found:" % (len(simple_cycles),))
        print(simple_cycles)

    # Break simple cycles and self-references to help find bigger cycles
    copy_of_graph = table_graph.copy()
    db_graph.break_cycles(copy_of_graph)

    try:
        cycle = nx.find_cycle(copy_of_graph)
        print("\nAnother cycle was detected:")
        print(cycle)
    except nx.exception.NetworkXNoCycle:
        pass


def print_partition_info(table_graph: Any) -> None:
    """Traverse table/foreign-key dependency graph and look for disconnected sub-graphs."""
    sub_graphs = [table_graph.subgraph(c) for c in nx.connected_components(table_graph.to_undirected())]
    if len(sub_graphs) > 1:
        print("\nDependency graph can be partitioned into %s sub-graphs:" % (len(sub_graphs),))
        for graph in sub_graphs:
            print(graph.nodes())


def print_insertion_order(table_graph: Any) -> None:
    """Print tables in insertion order."""
    print("\nInsertion order:")
    print(db_graph.get_insertion_order(table_graph))


def graph_export_to_dot_file(table_graph: Any, name: str = 'dependency_graph') -> None:  # pragma: no cover
    """Print table/foreign-key dependency graph in graphviz's dot file format."""
    print('digraph %s {' % (name,))
    print("node[shape=plaintext];")
    print('rankdir=LR; ranksep=1.0; size="16.5, 11.7";\n')
    for node in table_graph.nodes():
        print("""{0} [label=<{1}>];""".format(
            node, print_table(node, [])))

    for node in table_graph.nodes():
        for neighbour in table_graph[node]:
            edge = table_graph[node][neighbour].get('name')
            print('"%s":name -> "%s":name [label="%s"];' % (node, neighbour, edge))
    print('\n}')


def print_table(name: str, columns: Optional[List[str]] = None, color: str = '#aec7e8') -> str:  # pragma: no cover
    """Print table details in HTML format supported by graphviz's dot file format."""
    if columns is None:
        columns = []
    columns_str = ""
    for column in columns:
        columns_str += """
        <tr>
            <td align='left'><b><i>{column}</i></b></td>
            <td align='left'></td>
            <td align='left'>{details}</td>
        </tr>
        """.format(column=column, details="")

    return """
    <table border="1" cellborder="0" cellpadding="2" cellspacing="0" bgcolor="white" color="#999999">
        <tr>
            <td colspan='2' bgcolor='{color}' align='left' port="name"><b><i>public.{name}</i></b></td>
            <td bgcolor='{color}' align='right'>[table]</td>
        </tr>
    {columns_str}
    </table>""".format(name=name, color=color, columns_str=columns_str)


def transferability(inspector: Any, schema: str) -> None:  # pragma: no cover
    """
    Check that tables in the database schema have enough constraints to support consistent export/import by pgmerge.

    Specifically checking that "alternate_key" feature can work.
    """
    surrogate_key_tables = []
    natural_key_tables = []
    transformable = []
    pk_contains_fk = []
    tables = sorted(inspector.get_table_names(schema))
    for table in tables:
        columns = inspector.get_columns(table, schema)
        fks = inspector.get_foreign_keys(table, schema)
        pks = [col for pk in inspector.get_pk_constraint(table, schema) for col in pk['constrained_columns']]
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
            surrogate_key_tables.append(table)
        elif auto_id and auto_transformable:
            transformable.append(table)
        else:
            natural_key_tables.append(table)

    print("\nSurrogate keys:\n", surrogate_key_tables)
    print("\nNatural keys:\n", natural_key_tables)
    print("\nAuto-transformable to natural keys:\n", transformable)
    print("\nPK contains FK:\n", pk_contains_fk)
    # graph_export_to_dot_file(build_fk_dependency_graph(
    #    inspector, schema, surrogate_key_tables), name='surrogate_key_tables')
    # graph_export_to_dot_file(build_fk_dependency_graph(
    #    inspector, schema, natural_key_tables), name='natural_key_tables')


def main(engine: Any, schema: Optional[str],
         warnings: bool, list_tables: bool, table_details: bool, partition: bool,
         cycles: bool, insert_order: bool, export_graph: bool, transferable: bool) -> None:
    """Main-method to process various CLI commands to inspect details of database schema."""
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
        table_graph = db_graph.build_fk_dependency_graph(inspector, schema)
    if partition:
        print_partition_info(table_graph)
    if cycles:
        print_cycle_info_and_break_cycles(table_graph)
    if insert_order:
        print_insertion_order(table_graph)
    if export_graph:
        graph_export_to_dot_file(table_graph)
