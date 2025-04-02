from rules.rewrite_rules import RewriteRule
from sqlglot import exp
from utils import *

# TODO: refine predicate pushdown rule to handle more cases like partial matches
class PredicatePushdownRule(RewriteRule):
    def match(self, graph, node_id, context=None):
        """
        Check if downstream (child) nodes share an identical predicate in their WHERE clause.
        Context is expected to be a dict mapping node_id to its parsed AST.
        Returns (True, rule_context) if at least two children share the same predicate;
        otherwise returns (False, None).
        """
        if context is None:
            context = {}
        children = list(graph.successors(node_id))
        # print(f"[INFO] Node {node_id} children: {children}")
        if len(children) < 2:
            return False, None

        # scans children to find a common predicate
        # assumes equivalence of whole WHERE clause, including order of expressions
        # does not handle complex expressions or nested queries
        # Scenario where this doesnt work: 1 child has different predicate and 
        # is scanned first. 
        common_predicate = None
        matching_children = []
        for child in children:
            child_ast = context.get(child)
            if child_ast is None:
                continue
            child_where = child_ast.find(exp.Where)
            if not child_where:
                # This child does not have a WHERE clause so rule does not match
                return False, None
            # did not find a way to compare expressions directly, convert to SQL
            predicate_sql = child_where.this.sql(dialect=REWRITER_DIALECT)
            # print(f"[INFO] child {child} Predicate SQL: {predicate_sql}")
            if common_predicate is None:
                common_predicate = predicate_sql
                matching_children.append(child)
            elif predicate_sql == common_predicate:
                matching_children.append(child)
            else:
                return False, None

        if len(matching_children) >= 2:
            # Return a context dict containing the common predicate and affected children.
            return True, {"common_predicate": common_predicate, "children": matching_children}
        return False, None

    def apply(self, graph, node_id, asts, context=None):
        """
        Apply predicate pushdown: push the common predicate from downstream nodes into the
        current node's AST and remove it from the children.
        """
        if context is None or not isinstance(context, dict):
            print(f"[ERROR] Invalid context: {context}")
            return 

        common_predicate_sql = context.get("common_predicate")
        children = context.get("children", [])

        # Parse the common predicate with a dummy query
        try:
            dummy_sql = f"SELECT * FROM t WHERE {common_predicate_sql}"
            dummy_ast = sqlglot.parse_one(dummy_sql, read=REWRITER_DIALECT)
            common_predicate_expr = dummy_ast.find(exp.Where).this
            # common_predicate_expr = exp.Where(
            #     this=
            # )
        except Exception as e:
            print(f"[ERROR] Failed to parse common predicate: {e}")
            return 

        # add predicate to current node 
        ast = asts.get(node_id)
        current_where = ast.find(exp.Where)
        if current_where:
            # Combine with the existing predicate
            new_predicate = exp.And(this=current_where.this, expression=common_predicate_expr)
            current_where.set("this", new_predicate)
        else:
            new_where = exp.Where(this=common_predicate_expr)
            ast.set("where", new_where)

        # For each child remove its whole WHERE clause for now
        for child in children:
            child_ast = asts.get(child)
            if child_ast:
                child_where = child_ast.find(exp.Where)
                if child_where and \
                    child_where.this.sql(dialect=REWRITER_DIALECT) == \
                    common_predicate_sql:
                    child_ast.args.pop("where", None)

        # print what nodes are affected
        print(f"[INFO] Predicate pushdown applied at node {node_id} to children: {children}")
        