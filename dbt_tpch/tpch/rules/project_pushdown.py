# projection_pushdown_rule.py
import sqlglot
from sqlglot import exp
from sqlglot.optimizer import normalize
from rules.rewrite_rules import RewriteRule
from utils import REWRITER_DIALECT


class ProjectionPushdownRule(RewriteRule):
    """

    Current assumptions:
      * Parent SELECT items are either STAR (*) or simple Column refs
      * No complex expressions in parent's own SELECT list
    """

    def _required_columns_from_child(self, child_ast: exp.Expression) -> set[str]:
        # assume alias cannot and should not be pushed down
        alias_names = {
            item.alias_or_name
            for item in child_ast.expressions
            if isinstance(item, exp.Alias)
        }

        required = set()
        for col in child_ast.find_all(exp.Column):
            col_sql = col.sql(dialect=REWRITER_DIALECT)

            if col_sql in alias_names:
                continue

            required.add(col_sql)

        return required




    def match(self, graph, node_id, context=None):
        if context is None:
            context = {}

        # parent AST -----------------------------------------------------
        parent_ast = context.get(node_id)
        if parent_ast is None or not isinstance(parent_ast, exp.Select):
            return False, None

        # TODO: we assume parent projection list must be either STAR or just Columns
        parent_selects = parent_ast.expressions
        if not parent_selects:  # weird edge‑case
            return False, None

        # select * 
        parent_is_star = (
            len(parent_selects) == 1 and isinstance(parent_selects[0], exp.Star)
        )

        if not parent_is_star:
            # TODO: this is a navie case for now, parent has only columns
            if not all(isinstance(x, exp.Column) for x in parent_selects):
                return False, None

        # children -------------------------------------------------------
        children = list(graph.successors(node_id))
        if not children:
            return False, None

        required_cols: set[str] = set()
        for child in children:
            child_ast = context.get(child)
            if child_ast is None:
                continue

            # all cols referenced by child
            # TODO: this is a navie way, may not work on call cases like alias
            cols = self._required_columns_from_child(child_ast)
            required_cols |= cols

        parent_cols = (
            set()
            if parent_is_star
            else {c.sql(dialect=REWRITER_DIALECT) for c in parent_selects}
        )

        # log what parent cols and children cols are
        print(f"[INFO] Parent cols: {sorted(parent_cols)}")
        print(f"[INFO] Children cols: {sorted(required_cols)}")

        if required_cols and required_cols != parent_cols:
            return True, {"required_cols": required_cols}

        return False, None



    def apply(self, graph, node_id, asts, context=None):
        if not context or "required_cols" not in context:
            return

        required_cols = context["required_cols"]

        parent_ast = asts.get(node_id)
        if parent_ast is None:
            return

        # build new SELECT item list
        new_items = [
            sqlglot.parse_one(col_sql, read=REWRITER_DIALECT) for col_sql in required_cols
        ]

        # actual ast modification
        parent_ast.set("expressions", new_items) 

        print(
            f"[INFO] Projection push down on node {node_id}: "
            f"now selects {sorted(required_cols)}"
        )
