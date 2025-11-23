import json
from .base_agent import BaseAgent

class ExplanationGeneratorAgent(BaseAgent):
    def run(self, data: dict) -> dict:
        summary = {
            "sql": data.get("sql", "")[:400],
            "bq_status": data.get("bq_status"),
            "extra": data.get("extra", {}),
        }
        prompt = f"""You are a reconciliation analyst.
Given the job summary, explain likely causes of mismatches and remediation steps.
Write 4-6 short bullet points.
Summary: {json.dumps(summary)}
"""
        txt = self.llm.chat(prompt)
        return {"explanation": txt}
