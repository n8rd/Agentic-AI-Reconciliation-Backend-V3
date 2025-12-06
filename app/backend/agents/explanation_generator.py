import json
from typing import Any, Dict, List, Optional

from .base_agent import BaseAgent
from backend.connectors.bigquery_connector import BigQueryConnector


class ExplanationGeneratorAgent(BaseAgent):
    """
    ExplanationGeneratorAgent

    Backward-compatible with the existing orchestrator, but enhanced to:
      - Execute the generated reconciliation SQL in BigQuery
      - Return the full result rows (list[dict]) in 'result'
      - Update 'bq_status' with a simple status string
      - Provide a compact 'summary' object for debugging / logging

    Inputs (from node_explain or equivalent):
      data: {
        "sql": str | None,
        "bq_status": str | None,
        "extra": dict (optional, any metadata you pass along)
      }

    Outputs (merged into ReconState by the graph):
      {
        "explanation": str,     # used by node_explain and UI
        "summary": dict,        # node_explain can fall back to this if needed
        "bq_status": str,       # ReconState.bq_status
        "result": list[dict],   # ReconState.result (JSON-serialisable rows)
      }
    """

    def run(self, data: dict) -> dict:
        # ------ 1) Extract inputs safely ------
        sql: str = (data.get("sql") or "").strip()
        previous_bq_status: Optional[str] = data.get("bq_status")
        extra: Dict[str, Any] = data.get("extra") or {}

        rows: List[Dict[str, Any]] = []
        bq_status: Optional[str] = previous_bq_status

        # ------ 2) Execute the SQL in BigQuery, if present ------
        if sql:
            try:
                bq = BigQueryConnector()

                # Your connector implements:
                #   def run_query(self, query: str) -> pd.DataFrame
                df = bq.run_query(sql)

                if df is not None:
                    rows = df.to_dict(orient="records")

                bq_status = f"OK: {len(rows)} row(s)"
            except Exception as e:
                # Do not raise – we still want to return an explanation and payload
                bq_status = f"ERROR: {str(e)}"
                extra["bq_error"] = str(e)
        else:
            # No SQL provided at all
            if not bq_status:
                bq_status = "NO_SQL"

        # ------ 3) Build a compact summary (for logging / LLM context) ------
        summary: Dict[str, Any] = {
            # Shortened SQL preview to avoid over-long prompts
            "sql_preview": sql[:400],
            "row_count": len(rows),
            "bq_status": bq_status,
        }

        # ------ 4) Ask the LLM for a human-readable explanation ------
        prompt = f"""You are a reconciliation analyst.
Given the job summary, explain likely causes of mismatches and remediation steps.
Write 4–6 short bullet points.

Summary: {json.dumps(summary, default=str)}
"""
        explanation_text = self.llm.chat(prompt)

        # ------ 5) Return fields that the orchestrator already understands ------
        # node_explain does:
        #   state.explanation = eg_result.get("explanation") or eg_result.get("summary") or ...
        #
        # ReconState has:
        #   bq_status: str | None
        #   result: Any | None
        #
        # LangGraph merges this dict into the state, so no ReconState changes are required.
        return {
            "explanation": explanation_text,
            "summary": summary,
            "bq_status": bq_status,
            "result": rows,
        }

    # Some graph setups call agents as callables; keep this for safety.
    def __call__(self, data: dict) -> dict:
        return self.run(data)
