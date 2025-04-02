import json
import os
import networkx as nx
import sqlglot
from sqlglot import exp
from utils import *

class RewriteRule:
    def match(self, graph : nx.DiGraph, node_id, context:dict[str, exp.Expression]=None):
        """
        Given graph, node and optional context, determines if the rule applies.
        Returns a tuple (bool, rule_context) where rule_context is a dict.
        Return the rule_context however you like, maybe we'll streamline this in 
        a more defined pipeline later.
        """
        pass

    def apply(self, graph : nx.DiGraph, node_id, asts:dict[str, exp.Expression], context=None):
        """
        Given graph, node, its AST and optional context, apply a transformation.
        Should make changes directly to the passed asts dict + graph.
        """
        pass


