import json
import pandas as pd
from backend.utils.similarity import name_similarity
from .base_agent import BaseAgent

class SchemaMapperAgent(BaseAgent):
    def run(self, data: dict) -> dict:
        df_a: pd.DataFrame = data["df_a"]
        df_b: pd.DataFrame = data["df_b"]

        candidates = []
        for col_a in df_a.columns:
            for col_b in df_b.columns:
                score = name_similarity(col_a, col_b)
                candidates.append({"a_col": col_a, "b_col": col_b, "confidence": score})

        # Let LLM refine mapping if available
        prompt = f"""You are a schema mapping assistant.
Given candidate column pairs with similarity scores, choose best matches.
Return JSON: {{"matches":[{{"a_col":"","b_col":"","confidence":0.0}}]}}.
Candidates: {json.dumps(candidates[:50])}
"""
        raw = self.llm.chat(prompt)
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"matches": sorted(candidates, key=lambda x: x["confidence"], reverse=True)[:10]}
        return parsed
