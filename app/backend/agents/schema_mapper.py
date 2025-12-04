# backend/agents/schema_mapper.py

from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

from .base_agent import BaseAgent
from backend.utils.similarity import name_similarity
from backend.utils.similarity import normalize

from langchain_core.messages import HumanMessage


LLM_THRESHOLD = 0.65      # Used for deterministic fallback
FINAL_MATCH_THRESHOLD = 0.55  # LLM confidence cutoff


class SchemaMapperAgent(BaseAgent):
    """
    Hybrid schema mapper:
      1. Uses LLM to propose column pairs + confidence
      2. Uses deterministic matcher for safety fallback
      3. Combines results and enforces type correctness
      4. Produces stable, BigQuery-safe mapping
    """

    def run(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Support both df_a/df_b and data_a/data_b
        if "df_a" in data:
            df_a = data["df_a"]
        elif "data_a" in data:
            df_a = data["data_a"]
        else:
            raise ValueError(
                "SchemaMapperAgent.run expects key 'df_a' or 'data_a' in input data."
            )

        if "df_b" in data:
            df_b = data["df_b"]
        elif "data_b" in data:
            df_b = data["data_b"]
        else:
            raise ValueError(
                "SchemaMapperAgent.run expects key 'df_b' or 'data_b' in input data."
            )

        if df_a is None or df_b is None:
            raise ValueError(
                "SchemaMapperAgent.run expects 'df_a'/'df_b' or 'data_a'/'data_b' in the input data."
            )

        cols_a = df_a.columns.tolist()
        cols_b = df_b.columns.tolist()

        # classify A-side types
        numeric_a = set(df_a.select_dtypes(include=["number"]).columns.tolist())
        array_a = {
            c
            for c in df_a.select_dtypes(include=["object"]).columns
            if df_a[c].apply(lambda x: isinstance(x, list)).any()
        }
        string_a = set(df_a.select_dtypes(include=["object"]).columns.tolist()) - array_a

        # -------------------------------------------------------
        # 1) Ask LLM for semantic mapping suggestions
        # -------------------------------------------------------
        llm_prompt = (
            "You are a schema alignment expert.\n"
            "Given two column lists, produce the closest matching pairs.\n\n"
            f"Columns A: {cols_a}\n"
            f"Columns B: {cols_b}\n"
            "Return ONLY a JSON array of objects like:\n"
            "[{\"a\": \"colA\", \"b\": \"colB\", \"confidence\": 0.0 }, ...]"
        )

        llm_response = self.llm.invoke([HumanMessage(content=llm_prompt)])
        llm_pairs = self._safe_extract_llm_pairs(llm_response.content)

        # index LLM suggestions
        llm_map = {(p["a"], p["b"]): p["confidence"] for p in llm_pairs}

        # -------------------------------------------------------
        # 2) Deterministic fallback matching
        # -------------------------------------------------------
        det_candidates: List[Dict[str, Any]] = []
        for a_col in cols_a:
            best_b = None
            best_score = 0.0
            for b_col in cols_b:
                s = name_similarity(a_col, b_col)
                if s > best_score:
                    best_b = b_col
                    best_score = s

            if best_b and best_score >= LLM_THRESHOLD:
                det_candidates.append({
                    "a": a_col,
                    "b": best_b,
                    "confidence": best_score
                })

        # -------------------------------------------------------
        # 3) Merge LLM + deterministic
        # -------------------------------------------------------
        final_pairs: List[Dict[str, Any]] = []

        for a_col in cols_a:
            # LLM match?
            llm_match = [p for p in llm_pairs if p["a"] == a_col]
            if llm_match:
                best = max(llm_match, key=lambda x: x["confidence"])
                if best["confidence"] >= FINAL_MATCH_THRESHOLD:
                    final_pairs.append(best)
                    continue  # skip fallback

            # deterministic fallback
            det_match = [p for p in det_candidates if p["a"] == a_col]
            if det_match:
                final_pairs.append(det_match[0])

        # -------------------------------------------------------
        # 4) Classify final pairs (numeric / array / string)
        # -------------------------------------------------------
        matches = []
        numeric_cols = []
        array_cols = []
        string_cols = []

        for p in final_pairs:
            a = p["a"]
            b = p["b"]
            conf = float(p["confidence"])

            if a in numeric_a:
                t = "numeric"
                numeric_cols.append(a)
            elif a in array_a:
                t = "array"
                array_cols.append(a)
            elif a in string_a:
                t = "string"
                string_cols.append(a)
            else:
                t = "string"

            matches.append({
                "a_col": a,
                "b_col": b,
                "confidence": conf,
                "type": t,
            })

        return {
            "matches": matches,
            "numeric_cols": sorted(set(numeric_cols)),
            "array_cols": sorted(set(array_cols)),
            "string_cols": sorted(set(string_cols)),
        }

    # -------------------------------------------------------
    # Helper: parse JSON from LLM safely
    # -------------------------------------------------------
    def _safe_extract_llm_pairs(self, content: str) -> List[Dict[str, Any]]:
        import json

        try:
            parsed = json.loads(content)
            if isinstance(parsed, list):
                # ensure proper shape
                cleaned = []
                for p in parsed:
                    if "a" in p and "b" in p:
                        cleaned.append({
                            "a": p["a"],
                            "b": p["b"],
                            "confidence": float(p.get("confidence", 0.0))
                        })
                return cleaned
        except Exception:
            pass

        return []  # fallback
