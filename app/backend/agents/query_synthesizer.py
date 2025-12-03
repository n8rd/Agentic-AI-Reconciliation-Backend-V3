# backend/agents/query_synthesizer.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .base_agent import BaseAgent
from backend.utils.sql_templates import basic_reconciliation_sql


class QuerySynthesizerAgent(BaseAgent):
    def run(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expects data:
          {
            "schema_mapping": {
               "matches": [
                  {"a_col": "...", "b_col": "...", "confidence": 0.9, "type": "numeric"|"string"|"array"},
                  ...
               ],
               "numeric_cols": [...],  # A-side
               "array_cols":   [...],
               "string_cols":  [...],
            },
            "thresholds": {"abs": float, "rel": float},
            "table_a": "project.dataset.table_a",
            "table_b": "project.dataset.table_b",
            "columns_a": [...],   # list[str] from df_a.columns
            "columns_b": [...],   # list[str] from df_b.columns
          }
        """

        mapping: Dict[str, Any] = data.get("schema_mapping") or {}
        matches: List[Dict[str, Any]] = mapping.get("matches", []) or []

        thresholds: Dict[str, float] = data.get("thresholds") or {"abs": 0.01, "rel": 0.001}
        table_a: str = data["table_a"]
        table_b: str = data["table_b"]

        cols_a = set(data.get("columns_a") or [])
        cols_b = set(data.get("columns_b") or [])

        numeric_cols: List[str] = mapping.get("numeric_cols", []) or []
        array_cols: List[str] = mapping.get("array_cols", []) or []
        string_cols: List[str] = mapping.get("string_cols", []) or []

        # ------------------------------------------------------------------
        # 1) Build candidate join pairs from matches
        # ------------------------------------------------------------------
        join_candidates: List[Tuple[str, str]] = []

        for m in matches:
            a = m.get("a_col")
            b = m.get("b_col")
            if not a or not b:
                continue

            # Heuristic: consider it a join key if either side looks like an id
            if "id" in a.lower() or "id" in b.lower():
                join_candidates.append((a, b))

        # Fallback: first matched pair if no obvious ID-like columns
        if not join_candidates and matches:
            m0 = matches[0]
            a = m0.get("a_col")
            b = m0.get("b_col")
            if a and b:
                join_candidates.append((a, b))

        if not join_candidates:
            raise ValueError("No candidate join keys found from schema_mapping.matches")

        # Validate that join columns actually exist in A and B
        valid_join_pairs: List[Tuple[str, str]] = []
        missing_msgs: List[str] = []

        for (a, b) in join_candidates:
            ok = True
            if cols_a and a not in cols_a:
                missing_msgs.append(f"Join key '{a}' not found in table A")
                ok = False
            if cols_b and b not in cols_b:
                missing_msgs.append(f"Join key '{b}' not found in table B")
                ok = False
            if ok:
                valid_join_pairs.append((a, b))

        if not valid_join_pairs:
            detail = "; ".join(missing_msgs) if missing_msgs else "No valid join pairs"
            raise ValueError(f"Unable to choose join keys: {detail}")

        # ------------------------------------------------------------------
        # 2) Build numeric / array / string pairs based on mapping
        #    These can be asymmetric: e.g., work_city ↔ location
        # ------------------------------------------------------------------
        numeric_pairs: List[Tuple[str, str]] = []
        array_pairs: List[Tuple[str, str]] = []
        string_pairs: List[Tuple[str, str]] = []

        numeric_set = set(numeric_cols)
        array_set = set(array_cols)
        string_set = set(string_cols)

        for m in matches:
            a = m.get("a_col")
            b = m.get("b_col")
            if not a or not b:
                continue

            if a in numeric_set:
                numeric_pairs.append((a, b))
            elif a in array_set:
                array_pairs.append((a, b))
            elif a in string_set:
                string_pairs.append((a, b))

        # ------------------------------------------------------------------
        # 3) Build SQL using valid join pairs and metric pairs
        # ------------------------------------------------------------------
        sql = basic_reconciliation_sql(
            table_a=table_a,
            table_b=table_b,
            join_pairs=valid_join_pairs,
            numeric_pairs=numeric_pairs,
            thresholds=thresholds,
            array_pairs=array_pairs,
            string_pairs=string_pairs,
        )

        return {"sql": sql}
