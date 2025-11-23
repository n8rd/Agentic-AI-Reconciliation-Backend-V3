from .llm_provider import LLMProvider

class MockLLM(LLMProvider):
    def chat(self, prompt: str) -> str:
        return "MOCK_RESPONSE: " + prompt[:400]
