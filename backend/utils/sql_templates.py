from typing import Dict, List

ARRAY_UDFS = """CREATE TEMP FUNCTION ARRAY_EXCEPT_CUSTOM(a ARRAY<STRING>, b ARRAY<STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  const setB = new Set(b);
  return a.filter(x => !setB.has(x));
""";

CREATE TEMP FUNCTION ARRAY_DIFF_SCORE(a ARRAY<STRING>, b ARRAY<STRING>)
RETURNS FLOAT64
LANGUAGE js AS """
  if (!a || !b) return 0;
  const inter = a.filter(x => b.includes(x)).length;
  const union = new Set([...a, ...b]).size;
  return union === 0 ? 1 : inter / union;
""";"""


def basic_reconciliation_sql(
    table_a: str,
    table_b: str,
    join_keys: List[str],
    numeric_cols: List[str],
    thresholds: Dict,
    array_cols: List[str] | None = None,
) -> str:
    join_cond = " AND ".join([f"a.{k} = b.{k}" for k in join_keys])
    numeric_selects = []
    for c in numeric_cols:
        numeric_selects.append(f"""    ABS(a.{c} - b.{c}) AS {c}_abs_diff,
    SAFE_DIVIDE(ABS(a.{c} - b.{c}), NULLIF(ABS(b.{c}),0)) AS {c}_rel_diff""")

    numeric_where = []
    for c in numeric_cols:
        numeric_where.append(
            f"({c}_abs_diff > {thresholds.get('abs', 0.0)} OR {c}_rel_diff > {thresholds.get('rel', 0.0)})"
        )

    array_selects = []
    array_where = []
    array_cols = array_cols or []
    for c in array_cols:
        array_selects.append(f"ARRAY_DIFF_SCORE(a.{c}, b.{c}) AS {c}_array_score")
        array_where.append(f"{c}_array_score < 1.0")

    where_clauses = numeric_where + array_where
    where_clause = " OR ".join(where_clauses) if where_clauses else "FALSE"

    return f"""{ARRAY_UDFS}

WITH joined AS (
  SELECT
    a.*,
    b.*,
{',\n'.join(numeric_selects + array_selects)}
  FROM `{table_a}` a
  JOIN `{table_b}` b
    ON {join_cond}
)
SELECT *
FROM joined
WHERE {where_clause};
"""
