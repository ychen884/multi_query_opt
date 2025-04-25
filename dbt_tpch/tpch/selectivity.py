"""
Utility helpers for estimating predicate selectivity in DuckDB.
Supports two scenarios:
  1. Base table exists: use catalog statistics (duckdb_tables)
  2. Derived: compare the optimizer's estimated cardinality of the
     original SELECT vs. the same SELECT with an extra predicate.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Tuple

import duckdb
from sqlglot import exp
import sqlglot


DEFAULT_THRESHOLD: float = 0.25  # 25 % rows kept – change to taste

__all__ = [
    "estimate_selectivity",  # table‑based
    "should_pushdown",       # table‑based (bool, sel)
    "estimate_selectivity_ast",  # query‑based
    "should_pushdown_on_ast",    # query‑based (bool, sel)
    "DEFAULT_THRESHOLD",
]


_QNAME_RE = re.compile(r'"([^"\\]+)"|([^\.]+)')  # handles "db"."sch"."tbl" and db.sch.tbl


def _split_relation(qname: str):
    parts = [m.group(1) or m.group(2) for m in _QNAME_RE.finditer(qname)]
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def _first_card_node(plan_obj):
    """Walk dict *or* list, return first sub-dict that has EC."""
    if isinstance(plan_obj, dict):
        if (
            "extra_info" in plan_obj
            and "Estimated Cardinality" in plan_obj["extra_info"]
        ):
            return plan_obj
        for ch in plan_obj.get("children", []):
            hit = _first_card_node(ch)
            if hit:
                return hit
    elif isinstance(plan_obj, list):
        for item in plan_obj:
            hit = _first_card_node(item)
            if hit:
                return hit
    return None


def _estimated_rows(conn: duckdb.DuckDBPyConnection, query_sql: str) -> int:
    """Return the optimizer's estimated rows for *query_sql*.

    Handles the two-column EXPLAIN output `("physical_plan", json)` as well as
    single-column variants.  When the JSON comes back split across many rows we
    concatenate everything and pick the first JSON blob we see.
    """
    rows = conn.execute(f"EXPLAIN (FORMAT JSON) {query_sql}").fetchall()
    if not rows:
        return 0

    # flatten all textual cells into one big string
    merged = "".join(
            str(cell) for row in rows for cell in row if isinstance(cell, str)
        )

    start_brace = merged.find("{")
    start_bracket = merged.find("[")
    start = min([p for p in (start_brace, start_bracket) if p != -1]) if (
        start_brace != -1 or start_bracket != -1
    ) else -1
    if start == -1:
        raise ValueError("JSON blob not found in EXPLAIN output")

    json_str = merged[start:]
    plan_obj = json.loads(json_str)
    print("Parsed plan_obj:", plan_obj)

    node = _first_card_node(plan_obj)
    print("Found node with EC:", node)
    return int(node["extra_info"]["Estimated Cardinality"]) if node else 0

# ────────────────────────────────────────────────────────────────────────────────
# Table‑based: DO NOT USE, JUST FOR DEMO PURPOSES
# ────────────────────────────────────────────────────────────────────────────────

def estimate_selectivity(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    predicate_sql: str,
) -> float:
    """Fraction of rows that survive predicate on *existing* table."""
    db, schema, tbl = _split_relation(table_name)
    sql = (
        "SELECT estimated_size FROM duckdb_tables() WHERE table_name = ?"
        + (" AND schema_name   = ?" if schema else "")
        + (" AND database_name = ?" if db     else "")
    )
    params = [tbl] + ([schema] if schema else []) + ([db] if db else [])
    row = conn.execute(sql, params).fetchone()
    total_rows = row[0] if row and row[0] is not None else None
    if not total_rows:
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    est_rows = _estimated_rows(conn, f"SELECT * FROM {table_name} WHERE {predicate_sql}")
    return est_rows / max(total_rows, 1)


def should_pushdown(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    predicate_sql: str,
    threshold: float = DEFAULT_THRESHOLD,
) -> Tuple[bool, float]:
    sel = estimate_selectivity(conn, table_name, predicate_sql)
    return sel <= threshold, sel

# ────────────────────────────────────────────────────────────────────────────────
# Query‑based: based on before and after queries
# ────────────────────────────────────────────────────────────────────────────────

def _clone_with_extra_pred(base_ast: exp.Expression, predicate_sql: str) -> exp.Expression:
    """Return deep-copied AST with predicate_sql AND-ed into its WHERE."""
    new_ast = copy.deepcopy(base_ast)

    # Robust approach: parse predicate via a dummy query
    tmp = sqlglot.parse_one(f"SELECT * FROM _t WHERE {predicate_sql}")
    pred_ast = tmp.find(exp.Where).this

    where_node = new_ast.find(exp.Where)
    if where_node:
        combined = exp.And(this=where_node.this, expression=pred_ast)
        where_node.set("this", combined)
    else:
        new_ast.set("where", exp.Where(this=pred_ast))
    return new_ast


def estimate_selectivity_ast(
    conn: duckdb.DuckDBPyConnection,
    base_ast: exp.Expression,
    predicate_sql: str,
) -> float:
    """Return rows_with_pred / rows_without_pred (optimizer estimate)."""
    base_sql = base_ast.sql()
    rows_before = _estimated_rows(conn, base_sql)
    rows_before = max(rows_before, 1)  # avoid /0
    print(f"Rows before predicate: {rows_before}")

    new_ast = _clone_with_extra_pred(base_ast, predicate_sql)
    rows_after = _estimated_rows(conn, new_ast.sql())
    print(f"Rows after predicate: {rows_after}")
    return rows_after / rows_before


def should_pushdown_on_ast(
    conn: duckdb.DuckDBPyConnection,
    base_ast: exp.Expression,
    predicate_sql: str,
    threshold: float = DEFAULT_THRESHOLD,
) -> Tuple[bool, float]:
    sel = estimate_selectivity_ast(conn, base_ast, predicate_sql)
    print(f"Selectivity: {sel:.2%} (threshold: {threshold:.2%})")
    return sel <= threshold, sel
