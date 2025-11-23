import json
from .base_agent import BaseAgent

class EntityResolverAgent(BaseAgent):
    def run(self, data: dict) -> dict:
        entities = data.get("entities") or []
        if not entities:
            return {"pairs": []}
        prompt = f"""Resolve fuzzy textual entity names across two systems.
Return JSON: {{"pairs":[{{"left":"","right":"","confidence":0.0}}]}}.
Entities: {json.dumps(entities)}
"""
        raw = self.llm.chat(prompt)
        try:
            return json.loads(raw)
        except Exception:
            return {"pairs": []}
