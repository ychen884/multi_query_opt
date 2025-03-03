import json
import os
import networkx as nx
import sqlglot

def load_dbt_manifest(manifest_path):
    with open(manifest_path, "r") as f:
        return json.load(f)

def build_full_graph(manifest):
    """Build a full DAG of all models from the manifest."""
    G = nx.DiGraph()
    for node_id, node_data in manifest["nodes"].items():
        if node_data["resource_type"] == "model":
            G.add_node(node_id)
            for dep_id in node_data["depends_on"]["nodes"]:
                G.add_edge(dep_id, node_id)
    return G

def get_compiled_path(manifest, node_id):
    node_data = manifest["nodes"][node_id]
    return node_data.get("compiled_path")

def is_in_folder(manifest, node_id, folder_name):
    """
    Check if the compiled SQL path includes 'folder_name', e.g. "models/MQO_1".
    Adjust logic if your path check is different.
    """
    cpath = get_compiled_path(manifest, node_id)
    if cpath and folder_name in cpath:
        return True
    return False

def gather_subgraph(graph, manifest, folder_name):
    """
    Return a subgraph of all nodes that have compiled_path containing `folder_name`
    plus their upstream dependencies (if you want them).
    """
    # 1) Find all "target" nodes that are physically located in folder_name
    target_nodes = [n for n in graph if is_in_folder(manifest, n, folder_name)]
    
    # 2) For each target node, gather its upstream dependencies 
    #    so the subgraph is valid (no missing references).
    #    We'll do a Depth-First Search (DFS) or Breadth-First Search (BFS) up the graph.
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

def main():
    # Path to the manifest
    manifest_path = os.path.join("target", "manifest.json")
    if not os.path.isfile(manifest_path):
        print(f"[ERROR] {manifest_path} not found; run dbt compile first.")
        return

    # Load manifest
    manifest = load_dbt_manifest(manifest_path)

    # Build the full graph
    full_graph = build_full_graph(manifest)

    # Filter to only models in "models/MQO_1/"
    # (plus their upstream dependencies if any)
    folder_name = "models/MQO_1/"
    subG = gather_subgraph(full_graph, manifest, folder_name)

    # print nodes and edges
    print(f"Subgraph nodes ({len(subG.nodes)}):")
    for node in subG.nodes:
        print(f"  {node}")
    print(f"Subgraph edges ({len(subG.edges)}):")
    for edge in subG.edges:
        print(f"  {edge}")
    
    # Topological sort on the subgraph
    sorted_nodes = list(nx.topological_sort(subG))

    print("Filtered Subgraph (topological order):")
    for node in sorted_nodes:
        print(f"  {node}")

    # Parse each node's compiled SQL with SQLGlot
    for node_id in sorted_nodes:
        cpath = get_compiled_path(manifest, node_id)
        if cpath and os.path.isfile(cpath):
            with open(cpath) as f:
                sql_str = f.read()
            try:
                ast = sqlglot.parse_one(sql_str)
                print(f"\n---\nParsed AST for [{node_id}]:\n{ast.to_s()}")
            except Exception as e:
                print(f"[WARN] Could not parse [{node_id}]: {e}")

if __name__ == "__main__":
    main()
