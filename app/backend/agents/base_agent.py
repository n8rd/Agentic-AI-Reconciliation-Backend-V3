from abc import ABC, abstractmethod
from backend.providers.factory import get_llm

class BaseAgent(ABC):
    def __init__(self):
        self.llm = get_llm()

    @abstractmethod
    def run(self, data: dict) -> dict:
        ...
