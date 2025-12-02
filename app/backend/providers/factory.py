# providers/factory.py
from backend.config import settings

from .openai_provider import OpenAILLM
from .mock_provider import MockLLM
from .gemini_provider import GeminiLLM


def get_llm_provider():
    provider = settings.recon_model_provider.lower()

    if provider == "openai":
        return OpenAILLM()

    if provider == "gemini":
        return GeminiLLM()

    if provider == "mock":
        return MockLLM()

    raise ValueError(f"Unknown LLM provider: {provider}")

