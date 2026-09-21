"""
DE-JARVIS
J.A.R.V.I.S AI Data Engineering System
Backend Controller v4.0

This file connects:
    CommandAgent
        ->
    WorkflowPlanner
        ->
    ToolRegistry
        ->
    Orchestrator
        ->
    ArtifactManager

Windows-safe UTF-8 console output is enabled so the CLI can be
called directly or from Streamlit without cp1252 Unicode crashes.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any


# ============================================================
# WINDOWS / UTF-8 CONSOLE SAFETY
# ============================================================

def configure_utf8_output() -> None:
    """
    Prevent Windows cp1252 failures when JARVIS or its agents
    print Unicode such as arrows, dashes, or status symbols.
    """

    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)

        if stream is None:
            continue

        try:
            stream.reconfigure(
                encoding="utf-8",
                errors="replace",
            )
        except (AttributeError, OSError):
            pass

    # Helps subprocesses inherit UTF-8 behavior on Windows.
    os.environ.setdefault(
        "PYTHONIOENCODING",
        "utf-8",
    )


configure_utf8_output()


# ============================================================
# PROJECT IMPORTS
# ============================================================

from brain.command_agent import CommandAgent
from brain.workflow_planner import WorkflowPlanner
from orchestrator.orchestrator import Orchestrator

from tools.registry import tool_registry
from tools.data_tools import register_data_tools
from tools.artifact_manager import ArtifactManager


# ============================================================
# JARVIS
# ============================================================

class Jarvis:
    """
    Main JARVIS controller.

    Version 4.0 improvements:
        - Uses CommandAgent.parse_command() directly.
        - Windows UTF-8 safe output.
        - Stronger error handling.
        - Full command -> plan -> execute flow.
        - Artifact persistence.
        - Structured final result.
        - Streamlit/subprocess friendly.
        - No backend business logic duplicated here.
    """

    VERSION = "4.0"

    def __init__(self) -> None:

        self.project_root = (
            Path(__file__).resolve().parent.parent
        )

        self.command_agent = CommandAgent()

        self.workflow_planner = WorkflowPlanner()

        # Register the existing DE-JARVIS tools once.
        register_data_tools(
            tool_registry
        )

        self.orchestrator = Orchestrator(
            tool_registry
        )

        self.artifact_manager = ArtifactManager()

        self._initialized = True

    # ========================================================
    # SAFE PRINT
    # ========================================================

    @staticmethod
    def safe_print(
        value: Any = "",
        *,
        end: str = "\n",
    ) -> None:
        """
        Print safely even if the process is attached to a
        legacy Windows console.
        """

        text = str(value)

        try:

            print(
                text,
                end=end,
                flush=True,
            )

        except UnicodeEncodeError:

            safe_text = (
                text
                .encode(
                    "utf-8",
                    errors="replace",
                )
                .decode(
                    "utf-8",
                    errors="replace",
                )
            )

            try:

                print(
                    safe_text,
                    end=end,
                    flush=True,
                )

            except UnicodeEncodeError:

                ascii_text = (
                    text
                    .encode(
                        "ascii",
                        errors="replace",
                    )
                    .decode(
                        "ascii",
                        errors="replace",
                    )
                )

                print(
                    ascii_text,
                    end=end,
                    flush=True,
                )

    # ========================================================
    # SECTION
    # ========================================================

    def print_section(
        self,
        title: str,
    ) -> None:

        self.safe_print()
        self.safe_print(
            "=" * 72
        )
        self.safe_print(
            title
        )
        self.safe_print(
            "=" * 72
        )

    # ========================================================
    # STARTUP
    # ========================================================

    def show_startup(self) -> None:

        self.safe_print()
        self.safe_print(
            "=" * 72
        )
        self.safe_print(
            "             JARVIS AI DATA ENGINEERING SYSTEM"
        )
        self.safe_print(
            "=" * 72
        )

        self.safe_print(
            f"VERSION: {self.VERSION}"
        )

        self.safe_print(
            "Initializing JARVIS..."
        )

        self.safe_print(
            f"TOOLS REGISTERED: {tool_registry.count()}"
        )

        self.safe_print(
            "ARTIFACT MANAGER: ONLINE"
        )

        self.safe_print(
            "JARVIS STATUS: ONLINE"
        )

        self.safe_print()

    # ========================================================
    # COMMAND AGENT
    # ========================================================

    def parse_command(
        self,
        user_command: str,
    ) -> dict:

        if not user_command or not user_command.strip():

            raise ValueError(
                "User command cannot be empty."
            )

        # IMPORTANT:
        # Current CommandAgent public API is parse_command().
        return self.command_agent.parse_command(
            user_command.strip()
        )

    # ========================================================
    # SHOW COMMAND
    # ========================================================

    def show_command(
        self,
        command: dict,
    ) -> None:

        self.print_section(
            "COMMAND AGENT"
        )

        self.safe_print(
            f"Domain        : "
            f"{command.get('domain', 'UNKNOWN')}"
        )

        self.safe_print(
            f"Intent        : "
            f"{command.get('intent', 'UNKNOWN')}"
        )

        self.safe_print(
            f"Action        : "
            f"{command.get('action', 'UNKNOWN')}"
        )

        self.safe_print(
            f"Target        : "
            f"{command.get('target') or 'N/A'}"
        )

        confidence = command.get(
            "confidence",
            0,
        )

        try:
            confidence_value = float(
                confidence
            )
            confidence_text = (
                f"{confidence_value * 100:.0f}%"
                if confidence_value <= 1
                else f"{confidence_value:.0f}%"
            )
        except (
            TypeError,
            ValueError,
        ):
            confidence_text = str(
                confidence
            )

        self.safe_print(
            f"Confidence    : "
            f"{confidence_text}"
        )

        self.safe_print(
            f"Confirmation  : "
            f"{command.get('requires_confirmation', False)}"
        )

        parameters = command.get(
            "parameters"
        )

        if parameters:

            self.safe_print(
                f"Parameters    : "
                f"{json.dumps(parameters, ensure_ascii=False)}"
            )

    # ========================================================
    # WORKFLOW
    # ========================================================

    def create_workflow(
        self,
        command: dict,
    ) -> dict:

        return self.workflow_planner.create_plan(
            command
        )

    # ========================================================
    # SHOW WORKFLOW
    # ========================================================

    def show_workflow(
        self,
        workflow: dict,
    ) -> None:

        self.print_section(
            "WORKFLOW PLANNER"
        )

        self.safe_print(
            f"Workflow : "
            f"{workflow.get('workflow_name', 'N/A')}"
        )

        self.safe_print(
            f"Goal     : "
            f"{workflow.get('goal', 'N/A')}"
        )

        steps = workflow.get(
            "steps",
            []
        )

        self.safe_print(
            f"Steps    : "
            f"{len(steps)}"
        )

        self.safe_print(
            f"Confirm  : "
            f"{workflow.get('requires_confirmation', False)}"
        )

        self.safe_print()

        for index, step in enumerate(
            steps,
            start=1,
        ):

            action = step.get(
                "action",
                "UNKNOWN",
            )

            description = step.get(
                "description",
                "",
            )

            self.safe_print(
                f"{index}. "
                f"{action}"
                f" -> "
                f"{description}"
            )

    # ========================================================
    # EXECUTION
    # ========================================================

    def execute_workflow(
        self,
        workflow: dict,
    ) -> dict:

        return self.orchestrator.execute(
            workflow
        )

    # ========================================================
    # SHOW EXECUTION
    # ========================================================

    def show_execution(
        self,
        execution: dict,
    ) -> None:

        self.print_section(
            "ORCHESTRATOR"
        )

        self.safe_print(
            f"Status          : "
            f"{execution.get('status', 'UNKNOWN')}"
        )

        self.safe_print(
            f"Message         : "
            f"{execution.get('message', '')}"
        )

        completed = execution.get(
            "completed_steps",
            [],
        )

        self.safe_print(
            f"Completed Steps : "
            f"{len(completed)}"
        )

        failed_step = execution.get(
            "failed_step"
        )

        if failed_step:

            self.safe_print(
                f"Failed Step     : "
                f"{failed_step}"
            )

        self.safe_print()

        results = execution.get(
            "results",
            {},
        )

        for step_id, step_result in results.items():

            action = step_result.get(
                "action",
                "UNKNOWN",
            )

            status = step_result.get(
                "status",
                "UNKNOWN",
            )

            marker = (
                "OK"
                if status == "SUCCESS"
                else "FAIL"
            )

            self.safe_print(
                f"[{marker}] "
                f"{step_id:<10} "
                f"{action:<24} "
                f"{status}"
            )

            if status == "FAILED":

                error = step_result.get(
                    "error",
                    "Unknown error",
                )

                self.safe_print(
                    f"      Error: {error}"
                )

    # ========================================================
    # SHOW PIPELINE
    # ========================================================

    def show_pipeline(
        self,
        execution: dict,
    ) -> None:

        self.print_section(
            "PIPELINE"
        )

        results = execution.get(
            "results",
            {},
        )

        if not results:

            self.safe_print(
                "No pipeline steps executed."
            )

            return

        for step_id, step_result in results.items():

            self.safe_print(
                f"{step_id:<10} "
                f"{step_result.get('action', 'UNKNOWN'):<24} "
                f"{step_result.get('status', 'UNKNOWN')}"
            )

    # ========================================================
    # SHOW SCHEMA
    # ========================================================

    def show_schema(
        self,
        execution: dict,
    ) -> None:

        schema = execution.get(
            "schema"
        )

        if not schema:
            return

        self.print_section(
            "SCHEMA ANALYSIS"
        )

        columns = schema.get(
            "columns",
            [],
        )

        self.safe_print(
            f"{'COLUMN':<20}"
            f"{'TYPE':<15}"
            f"{'NULLABLE':<12}"
        )

        self.safe_print(
            "-" * 50
        )

        for column in columns:

            self.safe_print(
                f"{str(column.get('name', '')):<20}"
                f"{str(column.get('type', '')):<15}"
                f"{str(column.get('nullable', '')):<12}"
            )

    # ========================================================
    # SHOW SEMANTIC
    # ========================================================

    def show_semantic(
        self,
        execution: dict,
    ) -> None:

        semantic = execution.get(
            "semantic"
        )

        if not semantic:
            return

        self.print_section(
            "SEMANTIC ANALYSIS"
        )

        columns = semantic.get(
            "columns",
            [],
        )

        self.safe_print(
            f"{'COLUMN':<20}"
            f"{'ROLE':<18}"
            f"{'CONFIDENCE':<12}"
        )

        self.safe_print(
            "-" * 55
        )

        for column in columns:

            confidence = column.get(
                "confidence",
                0,
            )

            try:
                confidence_text = (
                    f"{float(confidence) * 100:.0f}%"
                )
            except (
                TypeError,
                ValueError,
            ):
                confidence_text = str(
                    confidence
                )

            self.safe_print(
                f"{str(column.get('name', '')):<20}"
                f"{str(column.get('role', '')):<18}"
                f"{confidence_text:<12}"
            )

    # ========================================================
    # SHOW QUALITY
    # ========================================================

    def show_quality(
        self,
        execution: dict,
    ) -> None:

        results = execution.get(
            "results",
            {},
        )

        quality = None

        for step in results.values():

            if step.get("action") == "DATA_QUALITY":

                quality = step.get(
                    "result"
                )

                break

        if not quality:
            return

        self.print_section(
            "DATA QUALITY"
        )

        self.safe_print(
            f"Status          : "
            f"{quality.get('status', 'UNKNOWN')}"
        )

        score = quality.get(
            "score"
        )

        if score is not None:

            self.safe_print(
                f"Score           : "
                f"{score}"
            )

        self.safe_print(
            f"Rows            : "
            f"{quality.get('row_count', 'N/A')}"
        )

        self.safe_print(
            f"Duplicate Rows  : "
            f"{quality.get('duplicate_rows', 'N/A')}"
        )

        self.safe_print(
            f"Duplicate IDs   : "
            f"{quality.get('duplicate_ids', 'N/A')}"
        )

    # ========================================================
    # SHOW VALIDATION
    # ========================================================

    def show_validation(
        self,
        execution: dict,
    ) -> None:

        results = execution.get(
            "results",
            {},
        )

        validation = None

        for step in results.values():

            if step.get("action") == "VALIDATE_DATA":

                validation = step.get(
                    "result"
                )

                break

        if not validation:
            return

        self.print_section(
            "DATA VALIDATION"
        )

        self.safe_print(
            f"Status : "
            f"{validation.get('status', 'UNKNOWN')}"
        )

        self.safe_print(
            f"Valid  : "
            f"{validation.get('valid', 'N/A')}"
        )

    # ========================================================
    # SHOW TRANSFORMATION
    # ========================================================

    def show_transformation(
        self,
        execution: dict,
    ) -> None:

        results = execution.get(
            "results",
            {},
        )

        transformation = None

        for step in results.values():

            if step.get("action") == "TRANSFORMATION":

                transformation = step.get(
                    "result"
                )

                break

        if not transformation:
            return

        self.print_section(
            "TRANSFORMATION"
        )

        self.safe_print(
            f"Rows : "
            f"{transformation.get('row_count', 'N/A')}"
        )

        rules = transformation.get(
            "rules",
            [],
        )

        for rule_group in rules:

            self.safe_print(
                "- "
                f"{rule_group.get('column')}"
                f" | role={rule_group.get('role')}"
                f" | confidence={rule_group.get('confidence')}"
                f" | rules={rule_group.get('rules')}"
            )

    # ========================================================
    # SHOW OUTPUTS
    # ========================================================

    def show_outputs(
        self,
        execution: dict,
        target: str | None,
    ) -> None:

        self.print_section(
            "DATA ENGINEERING OUTPUTS"
        )

        input_path = (
            f"data/raw/{target}"
            if target
            else "N/A"
        )

        silver_path = execution.get(
            "silver_path"
        )

        gold_path = execution.get(
            "gold_path"
        )

        # Current Orchestrator versions keep the Gold path
        # in execution. Older versions may only expose it
        # inside the GOLD_ANALYTICS step result.
        if not gold_path:

            for step in execution.get(
                "results",
                {},
            ).values():

                if step.get(
                    "action"
                ) == "GOLD_ANALYTICS":

                    result = step.get(
                        "result",
                        {}
                    )

                    if isinstance(
                        result,
                        dict
                    ):

                        gold_path = (
                            result.get("path")
                            or result.get("gold_path")
                        )

                    break

        self.safe_print(
            f"INPUT  : {input_path}"
        )

        self.safe_print(
            f"SILVER : {silver_path or 'N/A'}"
        )

        self.safe_print(
            f"GOLD   : {gold_path or 'N/A'}"
        )

    # ========================================================
    # SHOW GOLD
    # ========================================================

    def show_gold(
        self,
        execution: dict,
    ) -> None:

        results = execution.get(
            "results",
            {},
        )

        gold = None

        for step in results.values():

            if step.get(
                "action"
            ) == "GOLD_ANALYTICS":

                gold = step.get(
                    "result"
                )

                break

        if not gold:
            return

        rows = []

        if isinstance(
            gold,
            dict
        ):

            rows = gold.get(
                "data",
                []
            )

        if not rows:
            return

        self.print_section(
            "GOLD ANALYTICS"
        )

        if isinstance(
            rows,
            list
        ):

            for row in rows:

                if isinstance(
                    row,
                    dict
                ):

                    self.safe_print(
                        " | ".join(
                            f"{key}={value}"
                            for key, value
                            in row.items()
                        )
                    )

    # ========================================================
    # SAVE ARTIFACTS
    # ========================================================

    def save_artifacts(
        self,
        command: dict,
        workflow: dict,
        execution: dict,
        target: str | None,
    ) -> dict:

        if not target:

            return {}

        try:

            saved = (
                self.artifact_manager
                .save_pipeline_artifacts(
                    command=command,
                    workflow=workflow,
                    execution=execution,
                    file_name=target,
                )
            )

            return saved

        except Exception as error:

            # Artifact failure should not hide a successful
            # data pipeline.
            self.safe_print(
                f"WARNING: Artifact save failed: {error}"
            )

            return {}

    # ========================================================
    # SHOW ARTIFACTS
    # ========================================================

    def show_artifacts(
        self,
        artifacts: dict,
    ) -> None:

        if not artifacts:
            return

        self.print_section(
            "JARVIS ARTIFACTS"
        )

        for category, path in artifacts.items():

            self.safe_print(
                f"{category:<20}"
                f"{path}"
            )

    # ========================================================
    # FINAL STATUS
    # ========================================================

    def show_final(
        self,
        execution: dict,
        artifacts: dict,
    ) -> None:

        self.print_section(
            "FINAL STATUS"
        )

        status = execution.get(
            "status",
            "UNKNOWN"
        )

        self.safe_print(
            f"Status    : {status}"
        )

        self.safe_print(
            f"Silver    : "
            f"{execution.get('silver_path') or 'N/A'}"
        )

        gold_path = execution.get(
            "gold_path"
        )

        if not gold_path:

            for step in execution.get(
                "results",
                {},
            ).values():

                if step.get(
                    "action"
                ) == "GOLD_ANALYTICS":

                    result = step.get(
                        "result",
                        {}
                    )

                    if isinstance(
                        result,
                        dict
                    ):

                        gold_path = (
                            result.get("path")
                            or result.get("gold_path")
                        )

                    break

        self.safe_print(
            f"Gold      : "
            f"{gold_path or 'N/A'}"
        )

        self.safe_print(
            f"Artifacts : "
            f"{len(artifacts)}"
        )

    # ========================================================
    # MAIN RUN
    # ========================================================

    def run(
        self,
        user_command: str,
    ) -> dict:

        self.show_startup()

        self.print_section(
            "JARVIS"
        )

        self.safe_print(
            "USER COMMAND:"
        )

        self.safe_print(
            user_command
        )

        command = None
        workflow = None
        execution = None
        artifacts = {}

        try:

            # ------------------------------------------------
            # 1. COMMAND AGENT
            # ------------------------------------------------

            command = self.parse_command(
                user_command
            )

            self.show_command(
                command
            )

            # ------------------------------------------------
            # 2. CONFIRMATION
            # ------------------------------------------------

            requires_confirmation = (
                command.get(
                    "requires_confirmation",
                    False
                )
            )

            if requires_confirmation:

                self.safe_print()
                self.safe_print(
                    "ACTION REQUIRES CONFIRMATION."
                )

                return {
                    "status": "CONFIRMATION_REQUIRED",
                    "message": (
                        "Command requires confirmation."
                    ),
                    "command": command,
                }

            # ------------------------------------------------
            # 3. WORKFLOW PLANNER
            # ------------------------------------------------

            workflow = self.create_workflow(
                command
            )

            self.show_workflow(
                workflow
            )

            # ------------------------------------------------
            # 4. EXECUTE
            # ------------------------------------------------

            execution = self.execute_workflow(
                workflow
            )

            self.show_execution(
                execution
            )

            # ------------------------------------------------
            # 5. REPORTS
            # ------------------------------------------------

            self.show_pipeline(
                execution
            )

            self.show_schema(
                execution
            )

            self.show_semantic(
                execution
            )

            self.show_quality(
                execution
            )

            self.show_validation(
                execution
            )

            self.show_transformation(
                execution
            )

            target = command.get(
                "target"
            )

            self.show_outputs(
                execution,
                target
            )

            self.show_gold(
                execution
            )

            # ------------------------------------------------
            # 6. ARTIFACTS
            # ------------------------------------------------

            artifacts = self.save_artifacts(
                command,
                workflow,
                execution,
                target
            )

            self.show_artifacts(
                artifacts
            )

            # ------------------------------------------------
            # 7. FINAL
            # ------------------------------------------------

            self.show_final(
                execution,
                artifacts
            )

            return {
                "status": execution.get(
                    "status",
                    "UNKNOWN"
                ),
                "command": command,
                "workflow": workflow,
                "execution": execution,
                "artifacts": artifacts,
            }

        except Exception as error:

            self.print_section(
                "JARVIS ERROR"
            )

            self.safe_print(
                f"Type    : "
                f"{type(error).__name__}"
            )

            self.safe_print(
                f"Message : "
                f"{error}"
            )

            self.safe_print()

            self.safe_print(
                "TRACEBACK"
            )

            self.safe_print(
                "-" * 72
            )

            # traceback.print_exc() can itself hit console
            # encoding problems, so format it first and pass
            # through safe_print.
            formatted_traceback = (
                traceback.format_exc()
            )

            self.safe_print(
                formatted_traceback
            )

            return {
                "status": "FAILED",
                "message": str(error),
                "error_type": type(error).__name__,
                "command": command,
                "workflow": workflow,
                "execution": execution,
                "artifacts": artifacts,
            }


# ============================================================
# CLI
# ============================================================

def main() -> int:

    if len(sys.argv) > 1:

        user_command = " ".join(
            sys.argv[1:]
        ).strip()

    else:

        user_command = input(
            "JARVIS> "
        ).strip()

    if not user_command:

        print(
            "ERROR: No command supplied."
        )

        return 1

    jarvis = Jarvis()

    result = jarvis.run(
        user_command
    )

    jarvis.print_section(
        "JARVIS RESPONSE"
    )

    status = result.get(
        "status",
        "UNKNOWN"
    )

    jarvis.safe_print(
        f"STATUS: {status}"
    )

    if status == "SUCCESS":

        artifacts = result.get(
            "artifacts",
            {}
        )

        jarvis.safe_print(
            f"ARTIFACTS: {len(artifacts)}"
        )

        return 0

    if status == "CONFIRMATION_REQUIRED":

        jarvis.safe_print(
            "ACTION: CONFIRMATION REQUIRED"
        )

        return 2

    jarvis.safe_print(
        f"ERROR: "
        f"{result.get('message', 'Unknown error')}"
    )

    return 1


if __name__ == "__main__":
    sys.exit(
        main()
    )
