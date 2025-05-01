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
            return graph, asts

        # Create a new node
        cte_id = 0
        for cte, nodes in context["common_cte"].items():
            # Modify the graph
            # Make sure the node name is unique
            dummy_node_name = f"shared_cte_{cte_id}"

            graph.add_node(dummy_node_name)
            for node in nodes:
                graph.add_edge(dummy_node_name, node)

            # Create a new AST for the shared CTE
            asts[dummy_node_name] = cte.find(exp.Select).copy()

            # Remove the CTE from the original node
            # And replace the CTE reference with the new node
            for node in nodes:
                # Remove 
                ast = asts[node]
                with_expression = ast.find(exp.With)
                for node_cte in with_expression:
                    if cte == node_cte:
                        node_cte.pop()

                if len(with_expression.args["expressions"]) == 0:
                    with_expression.pop()

                print(repr(ast))

                table_ref = ast.find_all(exp.Table)
                for table in table_ref:
                    if table.this.this == cte.alias:
                        print(f"[INFO] Replacing {cte} with {dummy_node_name} in {node}")
                        table.this.replace(exp.Identifier(this=dummy_node_name))
                
                print(ast)

            cte_id += 1
