import json
import os
import networkx as nx
import sqlglot
from sqlglot import exp

# Assume duckdb dialect for now, used both for input & output sql
REWRITER_DIALECT = "duckdb"

def get_compiled_path(manifest, node_id):
    if node_id not in manifest["nodes"]:
        # print manifest nodes dict
        print(f"Manifest nodes:")
        for node_id, node_data in manifest["nodes"].items():
            print(f"  {node_id}: {node_data.get('name')}")
        raise Exception(f"Node {node_id} not found in manifest.")
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

def load_dbt_manifest(manifest_path):
    with open(manifest_path, "r") as f:
        return json.load(f)

_MANIFEST = None
def set_manifest(m):
    """Store the manifest once so other modules can look up relation names."""
    global _MANIFEST
    _MANIFEST = m

def relation_name(node_id: str) -> str | None:
    if _MANIFEST and node_id in _MANIFEST["nodes"]:
        return _MANIFEST["nodes"][node_id]["relation_name"]
    return None