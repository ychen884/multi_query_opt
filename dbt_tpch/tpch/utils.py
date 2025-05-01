import json
import os
import networkx as nx
import sqlglot
from sqlglot import exp

#### Constants ####
# Assume duckdb dialect for now, used both for input & output sql
REWRITER_DIALECT = "duckdb"
ENABLE_PARTIAL_MATCH = True

#### Utils ####
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

def filter_set_to_expr(filter_set : set[exp.Expression]) -> exp.Expression:
    """
    Convert a set of filters to an AND connected expression.
    """
    if len(filter_set) == 0:
        return None
    filters = list(filter_set)
    expr = filters[0]
    for filter in filters[1:]:
        expr = exp.And(this=expr, expression=filter)
    return expr
_MANIFEST = None
def set_manifest(m):
    """Store the manifest once so other modules can look up relation names."""
    global _MANIFEST
    _MANIFEST = m


def relation_name(node_id: str) -> str | None:
    if _MANIFEST and node_id in _MANIFEST["nodes"]:
        return _MANIFEST["nodes"][node_id]["relation_name"]
    return None

def forge_relation_name(node_id: str) -> str:
    """
    Forge a relation name from a node id using the typical dbt format.
    E.g. "dev.main.NODE_NAME"
    Might have consistency issues
    """
    tokens = ["dev", "main", node_id.split(".")[-1]]
    # add double quotes around each token then join with "."
    return ".".join([f'"{token}"' for token in tokens])

from dataclasses import dataclass, asdict
from typing import List, Dict, Any

@dataclass
class NewNodeRecord:
    node_id: str

_NEW_NODE_REGISTRY: Dict[str, NewNodeRecord] = {}

def register_new_node(record: NewNodeRecord) -> None:
    _NEW_NODE_REGISTRY[record.node_id] = record

def new_nodes() -> List[NewNodeRecord]:
    return list(_NEW_NODE_REGISTRY.values())

def get_new_node(node_id: str) -> NewNodeRecord | None:
    return _NEW_NODE_REGISTRY.get(node_id)

def clear_new_node_registry() -> None:
    _NEW_NODE_REGISTRY.clear()