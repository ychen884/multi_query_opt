import json
import os
import networkx as nx
import sqlglot

def main():
    # 1) Path to your manifest.json (adjust if needed)
    manifest_path = os.path.join("target", "manifest.json")

    if not os.path.isfile(manifest_path):
        print(f"[ERROR] Could not find manifest at {manifest_path}. "
              f"Please run `dbt compile` or `dbt run` first.")
        return

    # 2) Load the manifest JSON
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    # 3) Build a directed graph (DAG) of dbt models using networkx
    graph = nx.DiGraph()

    nodes = manifest.get("nodes", {})
    for node_id, node_data in nodes.items():
        # We only care about 'model' resource types here
        if node_data.get("resource_type") == "model":
            graph.add_node(node_id)
            # Add edges for upstream dependencies
            for dep_id in node_data["depends_on"]["nodes"]:
                graph.add_edge(dep_id, node_id)
    print("Directed graph of dbt models (upstream → downstream):")
    print(graph)
    print("Nodes:", graph.nodes)
    print("Edges:", graph.edges)
    print("--------------------------------")
    
    # 4) Topological sort of the DAG (models in execution order)
    sorted_nodes = list(nx.topological_sort(graph))
    print("DBT Models in topological order (upstream → downstream):")
    for node_id in sorted_nodes:
        print("  ", node_id)

    # 5) Parse compiled SQL with SQLGlot
    print("\nAttempting to parse each model's compiled SQL...")
    for node_id in sorted_nodes:
        node_data = nodes[node_id]
        compiled_path = node_data.get("compiled_path")

        if compiled_path and os.path.isfile(compiled_path):
            with open(compiled_path, "r") as fc:
                sql_str = fc.read()

            # Parse with SQLGlot
            try:
                ast = sqlglot.parse_one(sql_str)
                print(f"\n---\nParsed AST for [{node_id}]:\n{ast.to_s()}")
            except Exception as e:
                print(f"[WARN] Could not parse SQL for [{node_id}]: {e}")
        else:
            print(f"[WARN] No compiled SQL found for [{node_id}] at path: {compiled_path}")

if __name__ == "__main__":
    main()
