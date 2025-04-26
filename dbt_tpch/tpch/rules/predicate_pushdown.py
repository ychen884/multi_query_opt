import duckdb
import sqlglot.optimizer
import sqlglot.optimizer.normalize
import sqlglot.optimizer.simplify
from rules.rewrite_rules import RewriteRule
import sqlglot
from sqlglot import exp, optimizer
from utils import *
from selectivity import *

# TODO: refine predicate pushdown rule to handle more cases like partial matches
class PredicatePushdownRule(RewriteRule):
    def _flatten_where_clause(self, child_where_norm : exp.Where):
        """
        Flatten a where clause's predicate expression of exp1 and exp2 and ... 
        into a list of individual expressions.
        """
        result = set()
        for expr in child_where_norm.iter_expressions():
            result.update(set(expr.flatten(unnest=False)))
        return result
    
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
            # print(f"[INFO] Child {child} WHERE clause: {child_where_norm.sql(dialect=REWRITER_DIALECT)}")
            if ENABLE_PARTIAL_MATCH:
                if common_predicate is None:
                    common_predicate = self._flatten_where_clause(child_where_norm)
                    
                    # map each filter to the children that it came from.
                    # although now all children are required to match, this can be useful
                    # in case we want partially match the children in the future
                    for filter in common_predicate:         # non of these are as expected
                        # print(f"[INFO] 1st Filter: {filter.sql(dialect=REWRITER_DIALECT)}")
                        filter_to_child_map[filter] = set([child])
                else:
                    child_predicates = self._flatten_where_clause(child_where_norm)
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
            for filter in common_predicate: 
                print(f"[INFO] Final common predicate Filter: {filter.sql(dialect=REWRITER_DIALECT)}")
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

        if ENABLE_PARTIAL_MATCH:
            # this common_predicate is a set of predicate expressions
            common_predicate = context["common_predicate"]
            common_predicate_expr = filter_set_to_expr(common_predicate)
            print(f"[INFO] Common predicate expression: {common_predicate_expr}")
            common_predicate_sql = common_predicate_expr.sql(dialect=REWRITER_DIALECT)
        else:
            # this common_predicate is a complete where clause expression
            common_predicate_sql = context["common_predicate"].this.sql(dialect=REWRITER_DIALECT)
            common_predicate_expr = sqlglot.parse_one(common_predicate_sql, read=REWRITER_DIALECT).find(exp.Where).this
        
        # early-exit based on selectivity                               
        db_path = "dev.duckdb"
        con = duckdb.connect(db_path)
        
        base_ast  = asts[node_id]
        push, sel = should_pushdown_on_ast(con, base_ast, common_predicate_sql)
        if not push:
            print(f"[PredicatePushdownRule] Skip push-down(add intermediate node) on {node_id}: selectivity={sel:.2%} > "
                f"{DEFAULT_THRESHOLD:.0%}")
            con.close()
            # return
        con.close()
        
        children = context.get("children", [])
        print(f"[INFO] Pushdown common predicate '{common_predicate_sql}' " + 
                f"shared by children of node {node_id}: {children}")

        # add predicate to new node (TODO: add to current node if ephmeral) 
        # ast = asts.get(node_id)
        # current_where = ast.find(exp.Where)
        # if current_where:
        #     # Combine with the existing predicate
        #     new_predicate = exp.And(this=current_where.this, expression=common_predicate_expr)
        #     new_predicate = optimizer.simplify.simplify(new_predicate)
        #     current_where.set("this", new_predicate)
        # else:
        #     new_where = exp.Where(this=common_predicate_expr)
        #     ast.set("where", new_where)
        
        # if not ephemeral, create new node and add to graph
        node_name = node_id.split(".")[-1]
        new_node_name = f"{node_name}_pushdown"
        new_node_id = ".".join(node_id.split(".")[:-1] + [new_node_name])
        # add new node to graph by:
        # 1. edge from current node to new node
        # 2. and edge from new node to all children
        # 3. and remove previous edges from current node to children
        graph.add_node(new_node_id)
        for child in children:
            graph.add_edge(new_node_id, child)
            graph.remove_edge(node_id, child)
        # create new node sql, from clause refers to parent node taken from first child
        child_from = asts[children[0]].find(exp.From)
        asts[new_node_id] = sqlglot.parse_one(
            f"SELECT * FROM {child_from.this.sql(dialect=REWRITER_DIALECT)}", 
            read=REWRITER_DIALECT
        )
        asts[new_node_id].set("where", exp.Where(this=common_predicate_expr))
        # print new node sql
        print(f"[INFO] New node SQL: {asts[new_node_id].sql(dialect=REWRITER_DIALECT)}")

        if ENABLE_PARTIAL_MATCH:
            for child in children:
                child_ast = asts.get(child)
                if not child_ast: continue
                child_where = child_ast.find(exp.Where)
                if not child_where: continue
                child_where_norm = optimizer.normalize.normalize(child_where, dnf=False)
                child_where_norm = optimizer.simplify.simplify(child_where_norm).copy()
                child_predicates = self._flatten_where_clause(child_where_norm)
                new_predicates = child_predicates - common_predicate # set difference
                new_where = exp.Where(this=filter_set_to_expr(new_predicates))
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

        # update children from clause to use new node
        new_node_relation_name = forge_relation_name(new_node_id)
        print(f"[INFO] New node relation name: {new_node_relation_name}")
        for child in children:
            child_ast = asts.get(child)
            if child_ast:
                child_from = child_ast.find(exp.From)
                if child_from:
                    child_from.set("this", exp.Identifier(this=new_node_relation_name))
        
        # print what nodes are affected
        print(f"[INFO] Predicate pushdown applied at node {node_id} to children: {children}")
        