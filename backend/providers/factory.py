from backend.config import settings
from .openai_provider import OpenAILLM
from .mock_provider import MockLLM

def get_llm():
    if settings.model_provider == "openai":
        OpenAILLM.init_client()
        return OpenAILLM()
    return MockLLM()
