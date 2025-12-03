# backend/agents/query_synthesizer.py

import json
from .base_agent import BaseAgent
from backend.utils.sql_templates import basic_reconciliation_sql

class QuerySynthesizerAgent(BaseAgent):
    def run(self, data: dict) -> dict:
        """
        data fields expected:
          - schema_mapping   (dict with matches: [{a_col, b_col}, ...])
          - thresholds       ({abs, rel})
          - table_a          (FQN string)
          - table_b          (FQN string)
          - numeric_cols     (list of numeric columns)
          - array_cols       (list of array columns)
          - columns_a        (list of columns in table A)  # OPTIONAL for validation
          - columns_b        (list of columns in table B)  # OPTIONAL for validation
        """

        mapping = data["schema_mapping"]
        thresholds = data.get("thresholds", {"abs": 0.01, "rel": 0.001})
        table_a = data["table_a"]
        table_b = data["table_b"]
        numeric_cols = data.get("numeric_cols", [])
        array_cols = data.get("array_cols", [])

        # ----------------------------
        # 1) Build join_key pairs
        # ----------------------------
        join_pairs = []

        for m in mapping.get("matches", []):
            a = m["a_col"]
            b = m["b_col"]

            # Heuristics: consider as join keys if:
            #  - exact match
            #  - both contain "id"
            #  - either contains "id" AND columns actually exist in both tables
            if a.lower() == b.lower() or "id" in a.lower() or "id" in b.lower():
                join_pairs.append((a, b))

        # Fallback — use the first matched pair
        if not join_pairs and mapping.get("matches"):
            m = mapping["matches"][0]
            join_pairs = [(m["a_col"], m["b_col"])]

        if not join_pairs:
            raise ValueError("No suitable join keys found from schema mapping.")

        # ----------------------------
        # 2) Optional validation
        # ----------------------------
        cols_a = set(data.get("columns_a", []))
        cols_b = set(data.get("columns_b", []))

        for (a, b) in join_pairs:
            if cols_a and a not in cols_a:
                raise ValueError(f"Join key '{a}' not found in table A")
            if cols_b and b not in cols_b:
                raise ValueError(f"Join key '{b}' not found in table B")

        # ----------------------------
        # 3) Generate final SQL
        # ----------------------------
        sql = basic_reconciliation_sql(
            table_a=table_a,
            table_b=table_b,
            join_pairs=join_pairs,
            numeric_cols=numeric_cols,
            thresholds=thresholds,
            array_cols=array_cols,
        )

        return {"sql": sql}

