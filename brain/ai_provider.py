from abc import ABC, abstractmethod


class AIProvider(ABC):
    """
    Base interface for all AI providers.

    Future providers:
    - Groq
    - xAI
    - Local LLM
    """

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """
        Send a prompt to the AI provider
        and return the generated response.
        """
        pass