from .llm_provider import LLMProvider
from backend.config import settings
from openai import OpenAI

class OpenAILLM(LLMProvider):
    _client = None

    @classmethod
    def init_client(cls):
        if settings.openai_api_key:
            cls._client = OpenAI(
                api_key=settings.openai_api_key,
                base_url=settings.openai_base_url or "https://api.openai.com/v1"
            )

    def chat(self, prompt: str) -> str:
        if not OpenAILLM._client:
            from .mock_provider import MockLLM
            return MockLLM().chat(prompt)

        resp = OpenAI(api_key=settings.openai_api_key).chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": "Be precise, return JSON when asked, no markdown."},
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content
