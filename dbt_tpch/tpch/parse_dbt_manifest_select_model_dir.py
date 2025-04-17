import json
import os
import networkx as nx
import sqlglot
from sqlglot import exp

from rewriter import Rewriter
from rules.predicate_pushdown import PredicatePushdownRule
from rules.cse import CommonSubExpElimRule

from utils import *
import argparse

# tables materialized as output from sqls 
materialized_table_list = []

# topo sort order of sql files
topo_sort_order = []


# updated to not include source tables as nodes in graph
def build_full_graph(manifest):
    """Build a full DAG of all models from the manifest."""
    G = nx.DiGraph()
    for node_id, node_data in manifest["nodes"].items():
        if node_data["resource_type"] == "model":
            G.add_node(node_id)
            for dep_id in node_data["depends_on"]["nodes"]:
                if dep_id in manifest["nodes"]:
                    G.add_edge(dep_id, node_id)
    assert nx.is_directed_acyclic_graph(G), "Graph is not a DAG!"
    return G

def gather_subgraph(graph, manifest, folder_name):
    """
    Return a subgraph of all nodes that have compiled_path containing `folder_name`
    plus their upstream dependencies (if you want them).
    """
    # 1) Find all "target" nodes that are physically located in folder_name
    target_nodes = [n for n in graph if is_in_folder(manifest, n, folder_name)]
    all_needed = set()

    def visit_upstream(node):
        """Recursive DFS to gather all upstream dependencies of 'node'."""
        if node in all_needed:
            return
        all_needed.add(node)
        for pred in graph.predecessors(node):
            visit_upstream(pred)

    for tnode in target_nodes:
        visit_upstream(tnode)

    # 3) Now 'all_needed' is the set of nodes we want. Build the subgraph.
    subG = graph.subgraph(all_needed).copy()
    return subG
def extract_nodes_with_materialized_table(manifest):
    """
    Extracts the names of nodes from a manifest dictionary where the node's config
    has "materialized" set to "table".

    Parameters:
        manifest (dict): The manifest loaded from a JSON file.

    Returns:
        set: A set containing the relational names of the nodes that have "materialized": "table".
    """
    result = set()
    
    nodes = manifest.get("nodes", {})
    for node in nodes.values():
        config = node.get("config", {})
        if config.get("materialized") == "table":
            node_name = node.get("relation_name")
            if node_name:  # ensure the node has a name
                result.add(node_name)
    return result

def rewrite_ast_to_create_table(ast_obj, table_name, materialized_required_info):
    """
    Convert a SELECT-like AST into CREATE TABLE {table_name} AS (...) using sqlglot's expression classes.
    """
    # Ensure the AST is something we can embed as a subquery
    # For example, it might be a 'SELECT' or 'UNION' expression
    # If it's e.g. DDL or multiple statements, you may need extra logic.
    # print("Rewriting to add materialized table/view: ", table_name)
    # print("Materialized required info: ", materialized_required_info)
    materialized_type = "TABLE" if table_name in materialized_required_info else "VIEW"
    
    if materialized_type == "TABLE":
        materialized_table_list.append(table_name)
    create_expr = exp.Create(
        this=exp.Identifier(this=table_name),
        kind=materialized_type,
        expression=ast_obj,  # the parsed SELECT/CTE sub-tree
    )
    # print("Create expr: ", create_expr.to_s())
    
    create_sql = create_expr.sql(dialect=REWRITER_DIALECT)
    # remove unncessary ""xx""
    create_sql = create_sql.replace('""', '"')
     
    return create_sql

def main(folder_name=None):
    # Path to the manifest
    manifest_path = os.path.join("target", "manifest.json")
    if not os.path.isfile(manifest_path):
        print(f"[ERROR] {manifest_path} not found; run dbt compile first.")
        return

    # Load manifest
    manifest = load_dbt_manifest(manifest_path)

    # Build the full graph
    full_graph = build_full_graph(manifest)

    # Filter to only models in folder_name
    # (plus their upstream dependencies if any)
    # folder_name = "models/tpch_queries/"
    # folder_name = "models/MQO_1/"

    if folder_name:
        print(f"[INFO] Using only folder: {folder_name}")
        subG = gather_subgraph(full_graph, manifest, folder_name)
    else:
        print("[INFO] No folder specified; using entire graph.")
        subG = full_graph


    # print nodes and edges
    print(f"Subgraph nodes ({len(subG.nodes)}):")
    for node in subG.nodes:
        print(f"  {node}")
    print(f"Subgraph edges ({len(subG.edges)}):")
    for edge in subG.edges:
        print(f"  {edge}")

    # Extract the names of nodes that have "materialized": "table"
    materialized_required_info = extract_nodes_with_materialized_table(manifest)
    print(f"[INFO] materizlied nodes: {materialized_required_info}")
    
    # get_compiled_path(manifest, list(nx.topological_sort(subG))[0])
    # apply rewriter, potentially should use materialized_required_info
    print("[INFO] Applying rewriter...")
    rewriter = Rewriter(manifest, subG)
    rewriter.set_rules([
        # Add rewrite rules here
        PredicatePushdownRule(),
        CommonSubExpElimRule(),
    ])
    rewriter.run()
    print("[INFO] Rewriting DONE!")
    opt_subG = rewriter.graph   # in later versions this graph might have changed 
    opt_subG_asts = rewriter.asts
    
    # Topological sort on the subgraph
    sorted_nodes = list(nx.topological_sort(opt_subG))

    print("Filtered Subgraph (topological order):")
    for node in sorted_nodes:
        print(f"  {node}")
    
    # Parse each node's compiled SQL with SQLGlot
    for node_id in sorted_nodes:
        print(f"Processing node: {node_id}")
        
        # cpath = get_compiled_path(manifest, node_id)
        # print(cpath)
        
        output_folder = "optimized_sql"
        os.makedirs(output_folder, exist_ok=True)
        try:
            ast = opt_subG_asts[node_id]
            # print(f"\n---\nParsed AST for [{node_id}]:\n{ast.to_s()}")

            # (Done) TODO: find another way to store/get node data, since manifest is not updated if graph structure changes
            # TODO: verify whether this method is valid 
            dbt_relation_name = ""
            if node_id not in manifest["nodes"]:
                # If there is no node in the manifest, pick any node and get the database/schema name
                # Then, use the node_id as the relation name
                all_node_keys = list(manifest.get("nodes", {}).keys())
                first_node_id = all_node_keys[0]
                first_node_data = manifest["nodes"][first_node_id]
                first_node_dbt_relation_name = first_node_data.get("relation_name")
                dbt_relation_name_tokens = first_node_dbt_relation_name.split(".")
                dbt_relation_name_tokens[-1] = '"' + node_id + '"'
                dbt_relation_name = ".".join(dbt_relation_name_tokens)
                # (Done) TODO: May need to materialize as a table
                materialized_required_info.add(dbt_relation_name)
            else:
                node_data = manifest["nodes"][node_id]
                dbt_relation_name = node_data.get("relation_name")

            print(f"@@@ dbt_relation_name: {dbt_relation_name}")

            create_table_sql = rewrite_ast_to_create_table(ast, dbt_relation_name, materialized_required_info)
            print(f"@@@ Rewritten CREATE TABLE statement:\n{create_table_sql}\n")
            
            # base_filename = os.path.basename(cpath)
            base_filename = node_id.split(".")[-1] + ".sql"

            out_filename = f"optimized_sql_{base_filename}"
            out_path = os.path.join(output_folder, out_filename)
            with open(out_path, "w") as out_file:
                out_file.write(create_table_sql)
            print(f"[INFO] Wrote optimized SQL to: {out_path}\n")
            relative_path = os.path.relpath(out_path, start=os.getcwd())
            topo_sort_order.append(relative_path)
        except Exception as e:
            print(f"[WARN] Could not write parsed SQL for [{node_id}]: {e}")

    with open("materialized_tables_optimized.txt", "w") as f:
        for item in materialized_table_list:
            f.write("%s\n" % item)
    with open("topo_sort_order_optimized.txt", "w") as f:
        for sql_path in topo_sort_order:
            f.write(sql_path + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--folder",
        help="Optimize only the partial model in the specified folder. If omitted, optimize everything."
    )
    args = parser.parse_args()
    main(folder_name=args.folder)
