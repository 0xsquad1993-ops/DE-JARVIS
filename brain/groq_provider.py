import os

from dotenv import load_dotenv
from groq import Groq

from .ai_provider import AIProvider


class GroqProvider(AIProvider):
    """
    Groq implementation of the JARVIS AI provider.
    """

    def __init__(self, model=None):
        load_dotenv()

        api_key = os.getenv("GROQ_API_KEY")

        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY is not configured. "
                "Add GROQ_API_KEY to the .env file."
            )

        self.client = Groq(api_key=api_key)

        self.model = model or os.getenv(
            "GROQ_MODEL",
            "llama-3.3-70b-versatile"
        )

    def generate(self, prompt: str) -> str:
        """
        Send a prompt to Groq and return the AI response.
        """

        if not prompt or not prompt.strip():
            raise ValueError("Prompt cannot be empty.")

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
        )

        return response.choices[0].message.content