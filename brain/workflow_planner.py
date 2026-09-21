import json

from .groq_provider import GroqProvider


class WorkflowPlanner:
    """
    JARVIS Workflow Planner v1.5

    Converts a structured Command Agent result into
    an executable Data Engineering workflow.

    Supports:

    - Profiling
    - Validation
    - Transformation
    - Silver layer
    - Gold analytics
    - Full Raw -> Silver -> Gold pipeline
    - Deterministic file paths
    - Workflow validation
    - Safe JSON parsing
    """

    VERSION = "1.5"

    # =========================================================
    # SYSTEM PROMPT
    # =========================================================

    SYSTEM_PROMPT = """
You are JARVIS Workflow Planner v1.5.

You receive a structured command from the JARVIS Command Agent.

Your job is to convert that command into an executable
Data Engineering workflow.

AVAILABLE JARVIS TOOLS:

FILE:
- RESOLVE_CSV
- READ_CSV

DATA CLEANING:
- REMOVE_DUPLICATES
- CHECK_DUPLICATES

DATA ENGINEERING:
- SCHEMA_ANALYSIS
- SEMANTIC_ANALYSIS
- DATA_QUALITY
- VALIDATE_DATA
- TRANSFORMATION
- SAVE_SILVER
- GOLD_ANALYTICS


=========================================================
TOOL DEPENDENCIES
=========================================================

READ_CSV:

Requires:
- file

Produces:
- data
- file path


SCHEMA_ANALYSIS:

Requires:
- loaded CSV file

Produces:
- schema


SEMANTIC_ANALYSIS:

Requires:
- schema
- data

Produces:
- semantic schema


DATA_QUALITY:

Requires:
- data
- schema

Produces:
- quality result


VALIDATE_DATA:

Requires:
- data
- schema

Produces:
- validation result


TRANSFORMATION:

Requires:
- data
- semantic schema

Produces:
- transformed data
- transformation rules


SAVE_SILVER:

Requires:
- transformed data
- target/output path

Produces:
- saved Silver file


GOLD_ANALYTICS:

Requires:
- Silver file
- semantic schema
- target/output path

Produces:
- Gold analytics
- saved Gold file


REMOVE_DUPLICATES:

Requires:
- data

Produces:
- cleaned data


CHECK_DUPLICATES:

Requires:
- data

Produces:
- duplicate result


=========================================================
PROFILE WORKFLOW
=========================================================

For profile/analyze/inspect/understand dataset requests:

READ_CSV
    ->
SCHEMA_ANALYSIS
    ->
SEMANTIC_ANALYSIS
    ->
DATA_QUALITY


=========================================================
VALIDATION WORKFLOW
=========================================================

For validation requests:

READ_CSV
    ->
SCHEMA_ANALYSIS
    ->
SEMANTIC_ANALYSIS
    ->
DATA_QUALITY
    ->
VALIDATE_DATA


=========================================================
TRANSFORMATION WORKFLOW
=========================================================

For clean/transform/prepare/standardize/normalize/
silver requests:

READ_CSV
    ->
SCHEMA_ANALYSIS
    ->
SEMANTIC_ANALYSIS
    ->
DATA_QUALITY
    ->
VALIDATE_DATA
    ->
TRANSFORMATION
    ->
SAVE_SILVER


=========================================================
FULL PIPELINE WORKFLOW
=========================================================

For full pipeline / end-to-end / raw-to-gold /
complete pipeline requests:

READ_CSV
    ->
SCHEMA_ANALYSIS
    ->
SEMANTIC_ANALYSIS
    ->
DATA_QUALITY
    ->
VALIDATE_DATA
    ->
TRANSFORMATION
    ->
SAVE_SILVER
    ->
GOLD_ANALYTICS


=========================================================
GOLD WORKFLOW
=========================================================

For gold / analytics / aggregation requests:

READ_CSV
    ->
SCHEMA_ANALYSIS
    ->
SEMANTIC_ANALYSIS
    ->
DATA_QUALITY
    ->
VALIDATE_DATA
    ->
TRANSFORMATION
    ->
SAVE_SILVER
    ->
GOLD_ANALYTICS


=========================================================
OUTPUT PATHS
=========================================================

Silver output:

data/silver/<input_filename>

Gold output:

data/gold/<input_filename>

Never write to:

data/raw/


=========================================================
IMPORTANT RULES
=========================================================

1. Preserve the exact target filename.

2. Never invent an input filename.

3. READ_CSV must happen before processing.

4. TRANSFORMATION must happen after validation.

5. SAVE_SILVER must happen after transformation.

6. GOLD_ANALYTICS must happen after SAVE_SILVER.

7. GOLD_ANALYTICS must use the Silver file.

8. Never overwrite the raw source file.

9. Never write output to data/raw.

10. Use only available JARVIS tools.

11. Create steps in dependency order.

12. Return ONLY valid JSON.

The JSON must contain these fields:

workflow_name
goal
requires_confirmation
steps

Every step must contain:

step_id
action
description
tool
depends_on
"""

    # =========================================================
    # INIT
    # =========================================================

    def __init__(self, provider=None):

        self.provider = provider or GroqProvider()

    # =========================================================
    # CREATE PLAN
    # =========================================================

    def create_plan(self, command: dict) -> dict:
        """
        Create an executable workflow from
        Command Agent output.
        """

        if not isinstance(command, dict):

            raise TypeError(
                "Command must be a dictionary."
            )

        prompt = f"""
{self.SYSTEM_PROMPT}

COMMAND AGENT RESULT:

{json.dumps(command, indent=2)}

Create the executable workflow now.

Target:
{command.get("target")}

Action:
{command.get("action")}

Intent:
{command.get("intent")}

Domain:
{command.get("domain")}

Parameters:
{json.dumps(command.get("parameters", {}), indent=2)}

IMPORTANT:

If target is a CSV:

- preserve the exact filename
- READ_CSV must use that filename

For transformation workflows:

- include TRANSFORMATION
- include SAVE_SILVER
- Silver path must be data/silver/<input_filename>

For full pipeline workflows:

- include TRANSFORMATION
- include SAVE_SILVER
- include GOLD_ANALYTICS

Silver path:

data/silver/<input_filename>

Gold path:

data/gold/<input_filename>

GOLD_ANALYTICS must contain:

silver_file
gold_file

Return ONLY valid JSON.

Required workflow fields:

workflow_name
goal
requires_confirmation
steps

Required step fields:

step_id
action
description
tool
depends_on
"""

        # -----------------------------------------------------
        # Ask Groq
        # -----------------------------------------------------

        response = self.provider.generate(
            prompt
        )

        # -----------------------------------------------------
        # Parse AI response
        # -----------------------------------------------------

        plan = self._parse_json(
            response
        )

        # -----------------------------------------------------
        # Normalize AI output
        # -----------------------------------------------------

        plan = self._normalize_plan(
            plan,
            command
        )

        # -----------------------------------------------------
        # Inject deterministic target information
        # -----------------------------------------------------

        target = command.get(
            "target"
        )

        if target:

            for step in plan["steps"]:

                action = str(
                    step.get(
                        "action",
                        ""
                    )
                ).upper()

                # ---------------------------------------------
                # READ CSV
                # ---------------------------------------------

                if action == "READ_CSV":

                    step["file"] = target

                # ---------------------------------------------
                # SAVE SILVER
                # ---------------------------------------------

                elif action == "SAVE_SILVER":

                    step["output_path"] = (
                        f"data/silver/{target}"
                    )

                # ---------------------------------------------
                # GOLD ANALYTICS
                # ---------------------------------------------

                elif action == "GOLD_ANALYTICS":

                    step["silver_file"] = (
                        f"data/silver/{target}"
                    )

                    step["gold_file"] = (
                        f"data/gold/{target}"
                    )

        # -----------------------------------------------------
        # Final normalization
        # -----------------------------------------------------

        plan = self._normalize_plan(
            plan,
            command
        )

        # -----------------------------------------------------
        # Final validation
        # -----------------------------------------------------

        plan = self._validate_plan(
            plan
        )

        return plan

    # =========================================================
    # JSON PARSER
    # =========================================================

    def _parse_json(
        self,
        response: str
    ) -> dict:
        """
        Parse JSON from Groq response.

        This method ONLY parses JSON.

        Validation happens later.
        """

        if not response:

            raise ValueError(
                "Workflow Planner returned an empty response."
            )

        response = response.strip()

        # -----------------------------------------------------
        # Direct JSON
        # -----------------------------------------------------

        try:

            result = json.loads(
                response
            )

            if not isinstance(
                result,
                dict
            ):

                raise ValueError(
                    "Workflow Planner JSON must be an object."
                )

            return result

        except json.JSONDecodeError:

            pass

        # -----------------------------------------------------
        # Markdown JSON
        # -----------------------------------------------------

        cleaned = response

        if "```" in cleaned:

            cleaned = cleaned.replace(
                "```json",
                ""
            )

            cleaned = cleaned.replace(
                "```JSON",
                ""
            )

            cleaned = cleaned.replace(
                "```",
                ""
            )

            cleaned = cleaned.strip()

            try:

                result = json.loads(
                    cleaned
                )

                if not isinstance(
                    result,
                    dict
                ):

                    raise ValueError(
                        "Workflow Planner JSON must be an object."
                    )

                return result

            except json.JSONDecodeError:

                pass

        # -----------------------------------------------------
        # Find JSON object
        # -----------------------------------------------------

        start = response.find(
            "{"
        )

        end = response.rfind(
            "}"
        )

        if (
            start != -1
            and end != -1
            and end > start
        ):

            json_text = response[
                start:end + 1
            ]

            try:

                result = json.loads(
                    json_text
                )

                if not isinstance(
                    result,
                    dict
                ):

                    raise ValueError(
                        "Workflow Planner JSON must be an object."
                    )

                return result

            except json.JSONDecodeError:

                pass

        raise ValueError(
            "Workflow Planner returned invalid JSON:\n"
            f"{response}"
        )

    # =========================================================
    # NORMALIZE PLAN
    # =========================================================

    def _normalize_plan(
        self,
        plan: dict,
        command: dict
    ) -> dict:
        """
        Normalize AI output.

        Handles minor omissions from the AI while
        preserving deterministic command information.
        """

        if not isinstance(
            plan,
            dict
        ):

            raise ValueError(
                "Workflow Planner result must be a dictionary."
            )

        target = command.get(
            "target"
        )

        # -----------------------------------------------------
        # Workflow name
        # -----------------------------------------------------

        if not plan.get(
            "workflow_name"
        ):

            if target:

                plan["workflow_name"] = (
                    f"Workflow for {target}"
                )

            else:

                plan["workflow_name"] = (
                    "JARVIS Data Engineering Workflow"
                )

        # -----------------------------------------------------
        # Goal
        # -----------------------------------------------------

        if not plan.get(
            "goal"
        ):

            plan["goal"] = (
                command.get(
                    "intent"
                )
                or command.get(
                    "action"
                )
                or "Execute data engineering workflow"
            )

        # -----------------------------------------------------
        # Confirmation
        # -----------------------------------------------------

        if (
            "requires_confirmation"
            not in plan
        ):

            plan[
                "requires_confirmation"
            ] = bool(
                command.get(
                    "requires_confirmation",
                    False
                )
            )

        # -----------------------------------------------------
        # Steps
        # -----------------------------------------------------

        if "steps" not in plan:

            plan["steps"] = []

        if not isinstance(
            plan["steps"],
            list
        ):

            raise ValueError(
                "Workflow steps must be a list."
            )

        # -----------------------------------------------------
        # Normalize each step
        # -----------------------------------------------------

        for index, step in enumerate(
            plan["steps"],
            start=1
        ):

            if not isinstance(
                step,
                dict
            ):

                raise ValueError(
                    f"Workflow step {index} must be an object."
                )

            # -----------------------------------------------
            # Step ID
            # -----------------------------------------------

            if not step.get(
                "step_id"
            ):

                step["step_id"] = index

            # -----------------------------------------------
            # Action
            # -----------------------------------------------

            if step.get(
                "action"
            ):

                step["action"] = str(
                    step["action"]
                ).upper()

            # -----------------------------------------------
            # Tool
            # -----------------------------------------------

            if not step.get(
                "tool"
            ):

                step["tool"] = step.get(
                    "action"
                )

            if step.get(
                "tool"
            ):

                step["tool"] = str(
                    step["tool"]
                ).upper()

            # -----------------------------------------------
            # Description
            # -----------------------------------------------

            if not step.get(
                "description"
            ):

                step["description"] = (
                    f"Execute {step.get('action', 'workflow step')}"
                )

            # -----------------------------------------------
            # Dependencies
            # -----------------------------------------------

            if (
                "depends_on"
                not in step
            ):

                step["depends_on"] = []

            if not isinstance(
                step["depends_on"],
                list
            ):

                step["depends_on"] = []

        # -----------------------------------------------------
        # Deterministic target paths
        # -----------------------------------------------------

        if target:

            for step in plan["steps"]:

                action = str(
                    step.get(
                        "action",
                        ""
                    )
                ).upper()

                if action == "READ_CSV":

                    step["file"] = target

                elif action == "SAVE_SILVER":

                    step["output_path"] = (
                        f"data/silver/{target}"
                    )

                elif action == "GOLD_ANALYTICS":

                    step["silver_file"] = (
                        f"data/silver/{target}"
                    )

                    step["gold_file"] = (
                        f"data/gold/{target}"
                    )

        return plan

    # =========================================================
    # VALIDATE PLAN
    # =========================================================

    def _validate_plan(
        self,
        plan: dict
    ) -> dict:
        """
        Validate final workflow structure.
        """

        if not isinstance(
            plan,
            dict
        ):

            raise ValueError(
                "Workflow must be a dictionary."
            )

        # -----------------------------------------------------
        # Required workflow fields
        # -----------------------------------------------------

        required_fields = [
            "workflow_name",
            "goal",
            "requires_confirmation",
            "steps"
        ]

        for field in required_fields:

            if field not in plan:

                raise ValueError(
                    f"Workflow missing field: {field}"
                )

        # -----------------------------------------------------
        # Steps
        # -----------------------------------------------------

        if not isinstance(
            plan["steps"],
            list
        ):

            raise ValueError(
                "Workflow steps must be a list."
            )

        if not plan["steps"]:

            raise ValueError(
                "Workflow must contain at least one step."
            )

        # -----------------------------------------------------
        # Available tools
        # -----------------------------------------------------

        available_tools = {
            "RESOLVE_CSV",
            "READ_CSV",
            "REMOVE_DUPLICATES",
            "CHECK_DUPLICATES",
            "SCHEMA_ANALYSIS",
            "SEMANTIC_ANALYSIS",
            "DATA_QUALITY",
            "VALIDATE_DATA",
            "TRANSFORMATION",
            "SAVE_SILVER",
            "GOLD_ANALYTICS"
        }

        # -----------------------------------------------------
        # Step validation
        # -----------------------------------------------------

        step_ids = set()

        for index, step in enumerate(
            plan["steps"],
            start=1
        ):

            if not isinstance(
                step,
                dict
            ):

                raise ValueError(
                    f"Step {index} must be a dictionary."
                )

            required_step_fields = [
                "step_id",
                "action",
                "description",
                "tool",
                "depends_on"
            ]

            for field in required_step_fields:

                if field not in step:

                    raise ValueError(
                        f"Step {index} missing "
                        f"field: {field}"
                    )

            step_id = step[
                "step_id"
            ]

            # -----------------------------------------------
            # Unique step ID
            # -----------------------------------------------

            if step_id in step_ids:

                raise ValueError(
                    f"Duplicate step_id: {step_id}"
                )

            step_ids.add(
                step_id
            )

            # -----------------------------------------------
            # Normalize action
            # -----------------------------------------------

            step["action"] = str(
                step["action"]
            ).upper()

            # -----------------------------------------------
            # Normalize tool
            # -----------------------------------------------

            step["tool"] = str(
                step["tool"]
            ).upper()

            # -----------------------------------------------
            # Available tool check
            # -----------------------------------------------

            if step["tool"] not in available_tools:

                raise ValueError(
                    f"Step {step_id} uses unavailable "
                    f"tool: {step['tool']}"
                )

            # -----------------------------------------------
            # Dependency type
            # -----------------------------------------------

            if not isinstance(
                step["depends_on"],
                list
            ):

                raise ValueError(
                    f"Step {step_id} depends_on "
                    f"must be a list."
                )

            # -----------------------------------------------
            # READ_CSV
            # -----------------------------------------------

            if (
                step["action"]
                == "READ_CSV"
            ):

                if not step.get(
                    "file"
                ):

                    raise ValueError(
                        f"READ_CSV step {step_id} "
                        f"requires a file."
                    )

            # -----------------------------------------------
            # SAVE_SILVER
            # -----------------------------------------------

            if (
                step["action"]
                == "SAVE_SILVER"
            ):

                if not step.get(
                    "output_path"
                ):

                    raise ValueError(
                        f"SAVE_SILVER step {step_id} "
                        f"requires output_path."
                    )

                output_path = str(
                    step[
                        "output_path"
                    ]
                ).replace(
                    "\\",
                    "/"
                )

                if output_path.startswith(
                    "data/raw/"
                ):

                    raise ValueError(
                        "SAVE_SILVER cannot "
                        "write into data/raw."
                    )

                if not output_path.startswith(
                    "data/silver/"
                ):

                    raise ValueError(
                        "SAVE_SILVER must "
                        "write into data/silver."
                    )

            # -----------------------------------------------
            # GOLD_ANALYTICS
            # -----------------------------------------------

            if (
                step["action"]
                == "GOLD_ANALYTICS"
            ):

                if not step.get(
                    "silver_file"
                ):

                    raise ValueError(
                        f"GOLD_ANALYTICS step {step_id} "
                        f"requires silver_file."
                    )

                if not step.get(
                    "gold_file"
                ):

                    raise ValueError(
                        f"GOLD_ANALYTICS step {step_id} "
                        f"requires gold_file."
                    )

                silver_file = str(
                    step[
                        "silver_file"
                    ]
                ).replace(
                    "\\",
                    "/"
                )

                gold_file = str(
                    step[
                        "gold_file"
                    ]
                ).replace(
                    "\\",
                    "/"
                )

                # Gold cannot read raw
                if silver_file.startswith(
                    "data/raw/"
                ):

                    raise ValueError(
                        "GOLD_ANALYTICS cannot "
                        "read from data/raw."
                    )

                # Gold cannot write raw
                if gold_file.startswith(
                    "data/raw/"
                ):

                    raise ValueError(
                        "GOLD_ANALYTICS cannot "
                        "write into data/raw."
                    )

                # Gold must write Gold
                if not gold_file.startswith(
                    "data/gold/"
                ):

                    raise ValueError(
                        "GOLD_ANALYTICS must "
                        "write into data/gold."
                    )

            # -----------------------------------------------
            # Dependency validation
            # -----------------------------------------------

            for dependency in step[
                "depends_on"
            ]:

                if dependency not in step_ids:

                    raise ValueError(
                        f"Step {step_id} has invalid "
                        f"dependency: {dependency}"
                    )

        # -----------------------------------------------------
        # Confirmation normalization
        # -----------------------------------------------------

        plan[
            "requires_confirmation"
        ] = bool(
            plan[
                "requires_confirmation"
            ]
        )

        return plan


# ============================================================
# DIRECT TEST
# ============================================================

if __name__ == "__main__":

    print(
        "=" * 70
    )

    print(
        "JARVIS WORKFLOW PLANNER"
    )

    print(
        f"VERSION: {WorkflowPlanner.VERSION}"
    )

    print(
        "=" * 70
    )