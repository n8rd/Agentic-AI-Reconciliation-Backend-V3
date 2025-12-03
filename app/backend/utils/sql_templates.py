# backend/utils/sql_templates.py

from __future__ import annotations

from typing import Dict, List, Tuple

ARRAY_UDFS = r"""
CREATE TEMP FUNCTION ARRAY_EXCEPT_CUSTOM(a ARRAY<STRING>, b ARRAY<STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS ''' # Changed the inner delimiters to single quotes (''') to avoid conflicts
  if (!a || !b) return [];
  const setB = new Set(b);
  return a.filter(x => !setB.has(x));
''';

CREATE TEMP FUNCTION ARRAY_DIFF_SCORE(a ARRAY<STRING>, b ARRAY<STRING>)
RETURNS FLOAT64
LANGUAGE js AS ''' # Changed inner delimiters to single quotes (''')
  if (!a || !b) return 0;
  const inter = a.filter(x => b.includes(x)).length;
  const union = new Set([...a, ...b]).size;
  return union === 0 ? 1 : inter / union;
''';
"""


def basic_reconciliation_sql(
    table_a: str,
    table_b: str,
    join_pairs: List[Tuple[str, str]],
    numeric_pairs: List[Tuple[str, str]],
    thresholds: Dict,
    array_pairs: List[Tuple[str, str]] | None = None,
    string_pairs: List[Tuple[str, str]] | None = None,
) -> str:
    """
    table_a / table_b: fully-qualified table IDs
    join_pairs:   list of (a_col, b_col) used in JOIN
    numeric_pairs: list of (a_col, b_col) numeric comparisons
    array_pairs:   list of (a_col, b_col) array<STRING> comparisons
    string_pairs:  list of (a_col, b_col) string comparisons
    """

    array_pairs = array_pairs or []
    string_pairs = string_pairs or []

    # 1) JOIN condition
    join_cond = " AND ".join([f"a.{a} = b.{b}" for (a, b) in join_pairs])

    abs_thr = thresholds.get("abs", 0.0)
    rel_thr = thresholds.get("rel", 0.0)

    # 2) Numeric select + where
    numeric_selects: List[str] = []
    numeric_where: List[str] = []

    for (a_col, b_col) in numeric_pairs:
        abs_alias = f"{a_col}_abs_diff"
        rel_alias = f"{a_col}_rel_diff"

        numeric_selects.append(
f"""        ABS(a.{a_col} - b.{b_col}) AS {abs_alias},
        SAFE_DIVIDE(ABS(a.{a_col} - b.{b_col}), NULLIF(ABS(b.{b_col}), 0)) AS {rel_alias}"""
        )

        numeric_where.append(
            f"({abs_alias} > {abs_thr} OR {rel_alias} > {rel_thr})"
        )

    # 3) Array select + where
    array_selects: List[str] = []
    array_where: List[str] = []

    for (a_col, b_col) in array_pairs:
        score_alias = f"{a_col}_array_score"
        array_selects.append(
            f"        ARRAY_DIFF_SCORE(a.{a_col}, b.{b_col}) AS {score_alias}"
        )
        array_where.append(f"{score_alias} < 1.0")

    # 4) String select + where
    string_selects: List[str] = []
    string_where: List[str] = []

    for (a_col, b_col) in string_pairs:
        recon_alias = f"{a_col}_string_recon"
        string_selects.append(
f"""        CASE
            WHEN a.{a_col} IS NULL AND b.{b_col} IS NULL THEN 'MATCH'
            WHEN a.{a_col} IS NULL OR b.{b_col} IS NULL THEN 'MISMATCH'
            WHEN LOWER(CAST(a.{a_col} AS STRING)) = LOWER(CAST(b.{b_col} AS STRING)) THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS {recon_alias}"""
        )
        string_where.append(f"{recon_alias} = 'MISMATCH'")

    # 5) WHERE clause: any mismatch (numeric, array, string)
    where_clauses = numeric_where + array_where + string_where
    where_clause = " OR ".join(where_clauses) if where_clauses else "FALSE"

    # 6) Assemble SELECT projection
    all_metric_selects = numeric_selects + array_selects + string_selects
    if all_metric_selects:
        metrics_block = ",\n".join(all_metric_selects)
        select_body = f"""
    SELECT
        a.*,
        b.*,
{metrics_block}
"""
    else:
        select_body = """
    SELECT
        a.*,
        b.*
"""

    # 7) Final SQL
    return f"""
{ARRAY_UDFS}

WITH joined AS (
{select_body}
    FROM `{table_a}` a
    JOIN `{table_b}` b
      ON {join_cond}
)
SELECT *
FROM joined
WHERE {where_clause};
"""
