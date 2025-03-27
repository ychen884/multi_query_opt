import os
import networkx as nx
import sqlglot
from sqlglot import exp
import sqlglot.dialects
from utils import *
from rewrite_rules import RewriteRule

class Rewriter: 
    def __init__(self, manifest, subG : nx.DiGraph, rules=None):
        self.manifest = manifest
        self.graph = subG
        self.asts = {}
        self.rules = rules or []
        
    def set_rules(self, rules : list[RewriteRule]):
        self.rules = rules
        
    def run(self):
        # Process nodes in topological order to ensure dependency order
        sorted_nodes = list(nx.topological_sort(self.graph))
        for node_id in sorted_nodes:
            if node_id not in self.manifest["nodes"]:
                print(f"[WARN] Node {node_id} not found in manifest. Is source?")
                continue
            cpath = get_compiled_path(self.manifest, node_id)
            if cpath and os.path.isfile(cpath):
                with open(cpath, "r") as f:
                    sql_str = f.read()
                try:
                    print(f"[INFO] Parsing node {node_id}")
                    ast = sqlglot.parse_one(sql_str)
                    for ast_node in ast.walk():
                        ast_node.pop_comments()
                    self.asts[node_id] = ast
                except Exception as e:
                    # print(f"[ERROR] Could not parse node {node_id}: {e}")
                    raise Exception(f"Could not parse node {node_id}: {e}")
            else:
                # print(f"[WARN] File for node {node_id} not found or no compiled path.")
                raise Exception(f"File for node {node_id} not found or no compiled path.")
        # Apply rules (in order)
        # for now assume graph structure is not changed
        # TODO: handle graph structure changes
        # TODO: handle multiple/dynamic matches (loop until no more matches?)
        for rule in self.rules:
            for node_id in sorted_nodes:
                print(f"[INFO] Checking rule {rule.__class__.__name__} on node {node_id}")
                if node_id in self.asts:
                    ast = self.asts[node_id]
                    rule_matches, context = rule.match(self.graph, node_id, self.asts)
                    if rule_matches:
                        print(f"[INFO] Rule {rule.__class__.__name__} matched! Rewrite based at node {node_id}")
                        # self.asts[node_id] = rule.apply(self.graph, node_id, self.asts, context)
                        rule.apply(self.graph, node_id, self.asts, context)
                        print(f"[INFO] Base node rewritten SQL:\n{self.asts[node_id].sql(dialect=REWRITER_DIALECT)}")