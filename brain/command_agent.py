import json
import re

from .groq_provider import GroqProvider


class CommandAgent:
    """
    JARVIS Command Agent.

    Converts natural-language user requests into
    structured Data Engineering commands.
    """

    SYSTEM_PROMPT = """
You are JARVIS, an AI Data Engineering Command Agent.

Your job is to understand a user's natural-language request
and convert it into a structured Data Engineering command.

The user may speak in:
- English
- Tamil
- Tanglish
- Mixed language

You must understand the user's intent even when they
do not know technical Data Engineering terminology.

Available domains:

DATA_INGESTION
DATA_CLEANING
DATA_QUALITY
DATA_VALIDATION
DATA_TRANSFORMATION
SQL
PYSPARK
DATA_MODELING
ANALYTICS
PIPELINE
MONITORING
ERROR_RECOVERY
DATABRICKS
AZURE
ADF
FILE_OPERATION
GENERAL_DATA_ENGINEERING

Examples:

"remove duplicates"
→ DATA_CLEANING / REMOVE_DUPLICATES

"null values check pannu"
→ DATA_QUALITY / CHECK_NULLS

"இந்த csv clean பண்ணு"
→ DATA_CLEANING / CLEAN_DATA

"city wise salary average kudu"
→ ANALYTICS / GROUP_BY_AGGREGATION

"employees csv read pannu"
→ DATA_INGESTION / READ_CSV

"pipeline run pannu"
→ PIPELINE / RUN_PIPELINE

"databricks job run pannu"
→ DATABRICKS / RUN_JOB

"ADF pipeline create pannu"
→ ADF / CREATE_PIPELINE

"why pipeline failed?"
→ ERROR_RECOVERY / ANALYZE_FAILURE

Return ONLY valid JSON.

Required JSON structure:

{
    "domain": "DOMAIN_NAME",
    "intent": "INTENT_NAME",
    "action": "ACTION_NAME",
    "target": "TARGET_OR_NULL",
    "parameters": {},
    "confidence": 0.0,
    "requires_confirmation": false
}

Rules:

1. Never invent information that the user did not provide.
2. If target/file is unknown, use null.
3. Put additional details inside parameters.
4. confidence must be between 0.0 and 1.0.
5. Set requires_confirmation=true for destructive or
   production-impacting operations such as:
   DELETE, DROP, OVERWRITE_PRODUCTION,
   DELETE_PIPELINE, DELETE_DATA.
6. Normal read, profile, validate, analyze and test operations
   can use requires_confirmation=false.
7. Understand Tamil/Tanglish naturally.
"""

    def __init__(self, provider=None):
        self.provider = provider or GroqProvider()

    def parse_command(self, user_command: str) -> dict:
        """
        Convert a natural-language command into structured JSON.
        """

        if not user_command or not user_command.strip():
            raise ValueError("User command cannot be empty.")

        prompt = f"""
{self.SYSTEM_PROMPT}

USER COMMAND:
{user_command}

Return ONLY JSON.
"""

        response = self.provider.generate(prompt)

        return self._parse_json(response)

    def _parse_json(self, response: str) -> dict:
        """
        Safely extract JSON from the model response.
        """

        response = response.strip()

        # Direct JSON response
        try:
            result = json.loads(response)
            return self._validate_result(result)
        except json.JSONDecodeError:
            pass

        # JSON inside markdown/code block
        match = re.search(
            r"```(?:json)?\s*(\{.*?\})\s*```",
            response,
            re.DOTALL
        )

        if match:
            try:
                result = json.loads(match.group(1))
                return self._validate_result(result)
            except json.JSONDecodeError:
                pass

        # Find first JSON object
        start = response.find("{")
        end = response.rfind("}")

        if start != -1 and end != -1 and end > start:
            try:
                result = json.loads(response[start:end + 1])
                return self._validate_result(result)
            except json.JSONDecodeError:
                pass

        raise ValueError(
            f"JARVIS returned invalid JSON:\n{response}"
        )

    def _validate_result(self, result: dict) -> dict:
        """
        Validate the command-agent output structure.
        """

        required_fields = [
            "domain",
            "intent",
            "action",
            "target",
            "parameters",
            "confidence",
            "requires_confirmation"
        ]

        for field in required_fields:
            if field not in result:
                raise ValueError(
                    f"Command Agent response missing field: {field}"
                )

        if not isinstance(result["parameters"], dict):
            result["parameters"] = {}

        try:
            result["confidence"] = float(result["confidence"])
        except (TypeError, ValueError):
            result["confidence"] = 0.0

        result["confidence"] = max(
            0.0,
            min(1.0, result["confidence"])
        )

        result["requires_confirmation"] = bool(
            result["requires_confirmation"]
        )

        return result