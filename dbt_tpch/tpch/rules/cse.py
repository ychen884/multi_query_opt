from rules.rewrite_rules import RewriteRule
from sqlglot import exp

# TODO: implement common subexpression elimination rule
class CommonSubExpElimRule(RewriteRule):
    def match(self, graph, node_id, context = None):
        """
        Identify common CTEs and extract them as another materialized table
        """
        if context is None:
            context = {}

        # Traverse across all nodes in the graph and find common CTEs
        # TODO: We only need to apply this rule once unless other rules create new CTEs
        common_cte = {}
        for node in graph.nodes():
            ast = context.get(node)
            with_expression = ast.find(exp.With)
            if with_expression is not None:
                for cte in with_expression:
                    if cte not in common_cte:
                        common_cte[cte] = []
                    common_cte[cte].append(node)

        worth_common_cte = {}
        for cte, nodes in common_cte.items():
            if len(nodes) > 1:
                worth_common_cte[cte] = nodes

        if len(worth_common_cte) > 0:
            return True, {"common_cte": worth_common_cte}

        return False, None
    
    def apply(self, graph, node_id, asts, context=None):
        """
        Create a new node for the common CTE and replace all occurrences of the CTE in the graph
        """

        if context is None or not isinstance(context, dict):
            print(f"[ERROR] Invalid context: {context}")
            return 

        print(context["common_cte"])

        # Create a new node
        for cte, nodes in context["common_cte"].items():
            dummy_sql = cte.sql()
            print(dummy_sql)

        # Replace all occurrences of the CTE in the graph