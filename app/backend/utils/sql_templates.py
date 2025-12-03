from typing import Dict, List, Tuple

# ---------------------------------------------------------
# ARRAY DIFF UDFS
# ---------------------------------------------------------
ARRAY_UDFS = '''"""
CREATE TEMP FUNCTION ARRAY_EXCEPT_CUSTOM(a ARRAY<STRING>, b ARRAY<STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  if (!a || !b) return [];
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
""";
'''

# ---------------------------------------------------------
# MAIN RECON SQL
# ---------------------------------------------------------
def basic_reconciliation_sql(
    table_a: str,
    table_b: str,
    join_pairs: List[Tuple[str, str]],
    numeric_cols: List[str],
    thresholds: Dict,
    array_cols: List[str] | None = None,
) -> str:

    # -----------------------------------------------------
    # 1. JOIN CONDITION (supports asymmetric keys)
    # -----------------------------------------------------
    join_cond = " AND ".join([f"a.{a} = b.{b}" for (a, b) in join_pairs])

    # -----------------------------------------------------
    # 2. NUMERIC DIFF COLUMNS
    # -----------------------------------------------------
    numeric_selects = []
    numeric_where = []

    for col in numeric_cols:
        numeric_selects.append(f"""
        ABS(a.{col} - b.{col}) AS {col}_abs_diff,
        SAFE_DIVIDE(ABS(a.{col} - b.{col}), NULLIF(ABS(b.{col}), 0)) AS {col}_rel_diff
        """)

        numeric_where.append(
            f"({col}_abs_diff > {thresholds.get('abs', 0.0)} "
            f"OR {col}_rel_diff > {thresholds.get('rel', 0.0)})"
        )

    # -----------------------------------------------------
    # 3. ARRAY DIFF COLUMNS
    # -----------------------------------------------------
    array_cols = array_cols or []
    array_selects = []
    array_where = []

    for col in array_cols:
        array_selects.append(
            f"ARRAY_DIFF_SCORE(a.{col}, b.{col}) AS {col}_array_score"
        )
        # Any score < 1 means mismatch (not identical)
        array_where.append(
            f"{col}_array_score < 1.0"
        )

    # -----------------------------------------------------
    # 4. WHERE CLAUSE
    # -----------------------------------------------------
    where_clauses = numeric_where + array_where
    where_clause = " OR ".join(where_clauses) if where_clauses else "FALSE"

    # Combine selects cleanly
    selects = ",\n".join([s.strip() for s in numeric_selects + array_selects if s.strip()])

    # -----------------------------------------------------
    # 5. FINAL SQL
    # -----------------------------------------------------
    return f"""
{ARRAY_UDFS}

WITH joined AS (
    SELECT
        a.*,
        b.*,
        {selects}
    FROM `{table_a}` a
    JOIN `{table_b}` b
      ON {join_cond}
)
SELECT *
FROM joined
WHERE {where_clause};
"""