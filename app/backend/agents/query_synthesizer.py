import json
from .base_agent import BaseAgent
from backend.utils.sql_templates import basic_reconciliation_sql

class QuerySynthesizerAgent(BaseAgent):
    def run(self, data: dict) -> dict:
        mapping = data["schema_mapping"]
        thresholds = data.get("thresholds", {"abs": 0.01, "rel": 0.001})
        table_a = data["table_a"]
        table_b = data["table_b"]

        # naive: assume first match is key, others numeric/array discovered outside
        join_keys = [m["a_col"] for m in mapping.get("matches", []) if "id" in m["a_col"].lower()]
        if not join_keys and mapping.get("matches"):
            join_keys = [mapping["matches"][0]["a_col"]]

        numeric_cols = data.get("numeric_cols", [])
        array_cols = data.get("array_cols", [])

        sql = basic_reconciliation_sql(
            table_a=table_a,
            table_b=table_b,
            join_keys=join_keys,
            numeric_cols=numeric_cols,
            thresholds=thresholds,
            array_cols=array_cols,
        )
        return {"sql": sql}
