import duckdb
import sqlglot.optimizer
import sqlglot.optimizer.normalize
import sqlglot.optimizer.simplify
from rules.rewrite_rules import RewriteRule
import sqlglot
from sqlglot import exp, optimizer
from utils import REWRITER_DIALECT, relation_name 
from selectivity import *

# TODO: refine predicate pushdown rule to handle more cases like partial matches
class PredicatePushdownRule(RewriteRule):
    def match(self, graph, node_id, context=None):
        """
        Check if downstream (child) nodes share an identical predicate in their WHERE clause.
        Context is expected to be a dict mapping node_id to its parsed AST.
        Returns (True, rule_context) if at least two children share the same predicate;
        otherwise returns (False, None).
        """
        # print(f"[INFO] Matching PredicatePushdownRule at node {node_id}")
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
        filter_to_child_map = {}
        for child in children:
            child_ast = context.get(child)
            if child_ast is None:
                continue
            child_where = child_ast.find(exp.Where)
            if not child_where:
                # This child does not have a WHERE clause so rule does not match
                return False, None
    
            # did not find a way to compare expressions directly, convert to SQL
            # predicate_sql = child_where.this.sql(dialect=REWRITER_DIALECT)
            # print(f"[INFO] child {child} Predicate SQL: {predicate_sql}")
            # Currently uses CNF: (X or Y) and (X or Z) and ...
            child_where_norm = sqlglot.optimizer.normalize.normalize(child_where, dnf=False)
            # https://sqlglot.com/sqlglot/optimizer/simplify.html
            # for example: uniq_sort(node, root) is used in simplfy to resolve A and B == B and A
            # https://github.com/tobymao/sqlglot/blob/09882e32f057670a9cbd97c1e5cf1a00c774b5d2/sqlglot/optimizer/simplify.py#L105
            child_where_norm = sqlglot.optimizer.simplify.simplify(child_where_norm)
            print(f"[INFO] Child {child} WHERE clause: {child_where_norm.sql(dialect=REWRITER_DIALECT)}")
            if ENABLE_PARTIAL_MATCH:
                if common_predicate is None:
                    common_predicate = set(child_where_norm.flatten(unnest=False))
                    
                    # map each filter to the children that it came from.
                    # although now all children are required to match, this can be useful
                    # in case we want partially match the children in the future
                    # for filter in child_where_norm.iter_expressions():
                    # for filter in child_where_norm.find_all(exp.Or):
                    for filter in common_predicate:         # non of these are as expected
                        print(f"[INFO] Filter: {filter.sql(dialect=REWRITER_DIALECT)}")
                        filter_to_child_map[filter] = set([child])
                else:
                    child_predicates = set(child_where_norm.flatten(unnest=False))
                    common_predicate = common_predicate.intersection(child_predicates)
                    if len(common_predicate) == 0:
                        return False, None
                    else:
                        for filter in common_predicate:
                            filter_to_child_map[filter].add(child)                 
            else:
                if common_predicate is None:
                    common_predicate = child_where_norm
                    matching_children.append(child)
                else:
                    if child_where_norm == common_predicate:
                        # print(type(child_where_norm) is type(common_predicate) and child_where_norm.__hash__() == common_predicate.__hash__())
                        # print(f"[INFO] Found matching predicates child_where_norm: {child_where_norm.sql(dialect=REWRITER_DIALECT)}")
                        # print(f"common_predicate: {common_predicate.sql(dialect=REWRITER_DIALECT)}")
                        matching_children.append(child)
                    else:
                        return False, None
        # for partial match, uniquely add back matched children present in filter_to_child_map
        if ENABLE_PARTIAL_MATCH:
            matching_children_set = set()
            for filter in common_predicate:
                if filter in filter_to_child_map:
                    matching_children_set = matching_children_set.union(filter_to_child_map[filter])
            matching_children = list(matching_children_set)
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

        
        # early-exit based on selectivity                               
        common_predicate_sql = context["common_predicate"].this.sql(dialect=REWRITER_DIALECT)
        db_path = "dev.duckdb"
        con = duckdb.connect(db_path)
        
        base_ast  = asts[node_id]
        push, sel = should_pushdown_on_ast(con, base_ast, common_predicate_sql)
        if not push:
            print(f"[PredicatePushdownRule] Skip push-down(add intermediate node) on {node_id}: selectivity={sel:.2%} > "
                f"{DEFAULT_THRESHOLD:.0%}")
            con.close()
            return
        con.close()
        
        # common_predicate_sql = context.get("common_predicate")
        if ENABLE_PARTIAL_MATCH:
            # common_predicate is a set of expressions 
            common_predicate_expr = filter_set_to_expr(context.get("common_predicate"))
            common_predicate_sql = common_predicate_expr.sql(dialect=REWRITER_DIALECT)
        else:
            common_predicate_sql = context.get("common_predicate").this.sql(dialect=REWRITER_DIALECT)
            # Parse the common predicate with a dummy query
            try:
                dummy_sql = f"SELECT * FROM t WHERE {common_predicate_sql}"
                # print(f"[INFO] Parsing dummy SQL for common predicate: {dummy_sql}")
                dummy_ast = sqlglot.parse_one(dummy_sql, read=REWRITER_DIALECT)
                common_predicate_expr = dummy_ast.find(exp.Where).this
            except Exception as e:
                print(f"[ERROR] Failed to parse common predicate: {e}")
                return 
        children = context.get("children", [])
        print(f"[INFO] Pushdown common predicate '{common_predicate_sql}' " + 
                f"shared by children of node {node_id}: {children}")

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

        if ENABLE_PARTIAL_MATCH:
            for child in children:
                child_ast = asts.get(child)
                if not child_ast: continue
                child_where = child_ast.find(exp.Where)
                if not child_where: continue
                child_where_norm = sqlglot.optimizer.normalize.normalize(child_where, dnf=False)
                child_where_norm = sqlglot.optimizer.simplify.simplify(child_where_norm).copy()
                child_predicates = set(child_where_norm.flatten(unnest=False))
                new_predicates = child_predicates - context.get("common_predicate")
                new_where = filter_set_to_expr(new_predicates)
                child_ast.args.pop("where", None)
                child_ast.set("where", new_where)
        else:
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
        