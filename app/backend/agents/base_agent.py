from abc import ABC, abstractmethod
from backend.providers.factory import get_llm_provider

class BaseAgent(ABC):
    def __init__(self):
        self.llm = get_llm_provider()

    @abstractmethod
    def run(self, data: dict) -> dict:
        ...
