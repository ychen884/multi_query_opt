from rewrite_rules import RewriteRule

# TODO: implement shared table rule
class SharedTableRule(RewriteRule):
    def match(self, graph, node_id, context = None):
        pass
    
    def apply(self, graph, node_id, asts, context=None):
        pass