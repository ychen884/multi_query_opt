from rewrite_rules import RewriteRule

# TODO: implement common subexpression elimination rule
class CommonSubExpElimRule(RewriteRule):
    def match(self, graph, node_id, context = None):
        print(graph)
    
    def apply(self, graph, node_id, asts, context=None):
        pass