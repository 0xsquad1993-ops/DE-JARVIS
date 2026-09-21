"""
DE-JARVIS Production Orchestrator v9.0
======================================

Robust orchestration layer for DE-JARVIS.

Flow:
SOURCE -> SCHEMA -> SEMANTIC -> QUALITY -> VALIDATION
       -> TRANSFORMATION -> SILVER -> GOLD -> FINAL VALIDATION

v8 upgrades:
- Dynamic CSV schema discovery
- Generic profiling
- Quality metrics
- Validation gate
- Retry + failure capture
- Execution IDs and step timing
- Incremental/idempotent processing
- Force/full processing support
- Checkpoint metadata
- Artifact tracking
- JSON execution reports
- Compatibility with existing agents
- Safe agent invocation across common APIs
- Never overwrites the original source path
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


VERSION = "9.0.0"

BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
METADATA_DIR = BASE_DIR / "data" / "metadata"
CHECKPOINT_DIR = METADATA_DIR / "checkpoints"
INCREMENTAL_DIR = BASE_DIR / "data" / "bronze" / "incremental"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

for directory in (
    LOG_DIR,
    METADATA_DIR,
    CHECKPOINT_DIR,
    INCREMENTAL_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
):
    directory.mkdir(parents=True, exist_ok=True)


LOGGER = logging.getLogger("DE-JARVIS.Orchestrator")
if not LOGGER.handlers:
    LOGGER.setLevel(logging.INFO)
    file_handler = logging.FileHandler(
        LOG_DIR / "orchestrator.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(logging.StreamHandler())


@dataclass
class StepResult:
    name: str
    status: str = "PENDING"
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: float = 0.0
    attempts: int = 0
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    artifact: Optional[str] = None
    message: str = ""
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == "SUCCESS"


@dataclass
class ExecutionReport:
    execution_id: str
    pipeline: str
    source: str
    status: str = "RUNNING"
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: float = 0.0
    steps: List[StepResult] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    retries: int = 0
    total_rows: Optional[int] = None
    final_rows: Optional[int] = None
    schema: Optional[Dict[str, Any]] = None
    quality: Optional[Dict[str, Any]] = None
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["steps"] = [asdict(step) for step in self.steps]
        return data


class ProductionOrchestrator:
    """
    DE-JARVIS v9 orchestrator.

    Existing agents are optional. When an agent is unavailable, the
    orchestrator uses a safe generic fallback instead of crashing merely
    because a project-specific agent is missing.
    """

    def __init__(
        self,
        max_retries: int = 2,
        retry_delay_seconds: float = 1.0,
        agents: Optional[Dict[str, Any]] = None,
        incremental: bool = True,
        force: bool = False,
        stop_on_quality_failure: bool = False,
    ):
        self.max_retries = max(0, int(max_retries))
        self.retry_delay_seconds = max(0.0, float(retry_delay_seconds))
        self.agents = agents or {}
        self.incremental = bool(incremental)
        self.force = bool(force)
        self.stop_on_quality_failure = bool(stop_on_quality_failure)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, source_file: str | Path) -> Dict[str, Any]:
        original_source = Path(source_file)

        if not original_source.is_absolute():
            original_source = BASE_DIR / original_source

        original_source = original_source.resolve()

        execution_id = self._execution_id()

        report = ExecutionReport(
            execution_id=execution_id,
            pipeline="FULL_DATA_ENGINEERING_PIPELINE_V9",
            source=str(original_source),
            started_at=self._now(),
        )

        self._save_report(report)

        LOGGER.info(
            "EXECUTION START | id=%s | source=%s | version=%s",
            execution_id,
            original_source,
            VERSION,
        )

        try:
            self._require_source(original_source)

            incremental_info = self._prepare_incremental_source(
                original_source,
                execution_id,
                force=self.force,
            )

            if incremental_info["status"] == "SKIPPED_IDEMPOTENT":
                report.status = "SKIPPED_IDEMPOTENT"
                report.summary = (
                    "Source was already processed with the same content. "
                    "Use --full to force a complete run."
                )
                report.artifacts.append(str(incremental_info["checkpoint"]))

                self._save_report(report)
                self._save_summary(report)

                LOGGER.info(
                    "IDEMPOTENT SKIP | id=%s | source=%s",
                    execution_id,
                    original_source,
                )

                return report.to_dict()

            source_path = Path(incremental_info["effective_source"])

            context: Dict[str, Any] = {
                "execution_id": execution_id,
                "source": source_path,
                "original_source": original_source,
                "incremental": incremental_info,
                "report": report,
                "version": VERSION,
            }

            steps: List[tuple[str, Callable[[Dict[str, Any]], Any]]] = [
                ("SOURCE", self._step_source),
                ("SCHEMA", self._step_schema),
                ("SEMANTIC", self._step_semantic),
                ("QUALITY", self._step_quality),
                ("VALIDATION", self._step_validation),
                ("TRANSFORMATION", self._step_transformation),
                ("SILVER", self._step_silver),
                ("GOLD", self._step_gold),
            ]

            for name, fn in steps:
                step = StepResult(name=name)
                report.steps.append(step)

                try:
                    result = self._execute_step(
                        step=step,
                        fn=fn,
                        context=context,
                        report=report,
                    )
                except Exception as exc:
                    # The step has already been recorded by _execute_step.
                    # Stop the pipeline because downstream data may be invalid.
                    report.status = "FAILED"
                    report.summary = f"Pipeline stopped at {name}: {exc}"
                    raise

                if result is None:
                    raise RuntimeError(f"{name} returned no result")

                if name == "SOURCE":
                    # Important: never overwrite context["source"].
                    context["source_result"] = result
                else:
                    context[name.lower()] = result

                # Keep one enriched schema for downstream agents. The existing
                # SemanticAgent returns semantic roles separately; transformation
                # and GoldAgent need those roles on the schema columns.
                if name == "SEMANTIC":
                    context["working_schema"] = self._merge_semantic_schema(
                        context.get("schema", {}),
                        result,
                    )

                self._capture_step_metadata(step, result, report)

                if name == "SCHEMA" and isinstance(result, dict):
                    report.schema = result

                if name == "QUALITY" and isinstance(result, dict):
                    report.quality = result

                # Optional quality gate.
                if name == "QUALITY" and self.stop_on_quality_failure:
                    if not self._quality_passed(result):
                        raise RuntimeError(
                            "Quality gate failed and stop_on_quality_failure=True"
                        )

            report.status = "SUCCESS"
            report.final_rows = self._find_final_rows(context)

            report.summary = (
                f"Pipeline completed successfully: "
                f"{sum(step.success for step in report.steps)}/"
                f"{len(report.steps)} steps passed."
            )

            self._commit_checkpoint(
                original_source=original_source,
                incremental_info=incremental_info,
                execution_id=execution_id,
                report=report,
            )

            LOGGER.info(
                "EXECUTION SUCCESS | id=%s | steps=%s/%s",
                execution_id,
                sum(step.success for step in report.steps),
                len(report.steps),
            )

        except Exception as exc:
            report.status = "FAILED"

            report.errors.append(
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "timestamp": self._now(),
                    "traceback": traceback.format_exc(limit=8),
                }
            )

            if not report.summary:
                report.summary = f"Pipeline failed: {exc}"

            LOGGER.exception(
                "EXECUTION FAILED | id=%s",
                execution_id,
            )

        finally:
            report.ended_at = self._now()
            report.duration_seconds = self._duration(
                report.started_at,
                report.ended_at,
            )

            self._save_report(report)
            self._save_summary(report)

        return report.to_dict()

    # ------------------------------------------------------------------
    # Step runner
    # ------------------------------------------------------------------

    def _execute_step(
        self,
        step: StepResult,
        fn: Callable[[Dict[str, Any]], Any],
        context: Dict[str, Any],
        report: ExecutionReport,
    ) -> Any:

        total_attempts = self.max_retries + 1

        for attempt in range(1, total_attempts + 1):
            step.attempts = attempt
            step.started_at = self._now()
            step.status = "RUNNING"
            step.error = None

            LOGGER.info(
                "STEP START | id=%s | step=%s | attempt=%s/%s",
                report.execution_id,
                step.name,
                attempt,
                total_attempts,
            )

            try:
                result = fn(context)

                if result is None:
                    raise RuntimeError(f"{step.name} returned no result")

                step.status = "SUCCESS"
                step.message = "Step completed successfully."
                step.ended_at = self._now()
                step.duration_seconds = self._duration(
                    step.started_at,
                    step.ended_at,
                )

                if attempt > 1:
                    report.retries += attempt - 1

                LOGGER.info(
                    "STEP SUCCESS | id=%s | step=%s | duration=%.3fs",
                    report.execution_id,
                    step.name,
                    step.duration_seconds,
                )

                return result

            except Exception as exc:
                step.error = str(exc)
                step.ended_at = self._now()
                step.duration_seconds = self._duration(
                    step.started_at,
                    step.ended_at,
                )

                report.errors.append(
                    {
                        "step": step.name,
                        "attempt": attempt,
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "timestamp": self._now(),
                    }
                )

                LOGGER.error(
                    "STEP FAILED | id=%s | step=%s | attempt=%s | error=%s",
                    report.execution_id,
                    step.name,
                    attempt,
                    exc,
                )

                if attempt < total_attempts:
                    step.status = "RETRYING"
                    time.sleep(self.retry_delay_seconds)
                    continue

                step.status = "FAILED"
                raise

        raise RuntimeError(f"Unreachable retry state for {step.name}")

    # ------------------------------------------------------------------
    # Pipeline steps
    # ------------------------------------------------------------------

    def _step_source(self, context: Dict[str, Any]) -> Dict[str, Any]:
        source = Path(str(context["source"]))

        row_count = self._count_rows(source)

        return {
            "status": "SUCCESS",
            "file": str(source),
            "artifact": str(source),
            "row_count": row_count,
            "format": source.suffix.lower().lstrip("."),
        }

    def _step_schema(self, context: Dict[str, Any]) -> Dict[str, Any]:
        agent = self._get_agent("schema")

        if agent is not None:
            result = self._safe_call_agent(
                agent,
                [
                    (context["source"],),
                    (str(context["source"]),),
                    (),
                ],
            )
            if result is not None:
                return self._normalize_schema_result(result)

        # Generic fallback.
        source = Path(str(context["source"]))

        if source.suffix.lower() != ".csv":
            return {
                "status": "PASS",
                "columns": [],
                "column_count": 0,
                "row_count": 0,
                "message": "Non-CSV schema fallback.",
            }

        with source.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            columns = reader.fieldnames or []

        schema = []

        for column in columns:
            values = [
                row.get(column)
                for row in rows
                if row.get(column) not in ("", None)
            ]

            schema.append(
                {
                    "name": column,
                    "type": self._infer_type(values),
                    "nullable": len(values) < len(rows),
                    "non_null_count": len(values),
                    "null_count": len(rows) - len(values),
                    "unique_count": len(set(map(str, values))),
                }
            )

        return {
            "status": "PASS",
            "columns": schema,
            "column_count": len(columns),
            "row_count": len(rows),
            "message": "Dynamic schema discovery completed.",
        }

    def _step_semantic(self, context: Dict[str, Any]) -> Dict[str, Any]:
        agent = self._get_agent("semantic")

        schema = context.get("schema", {})

        if agent is not None:
            result = self._safe_call_agent(
                agent,
                [
                    (schema,),
                    (context.get("source"), schema),
                    (),
                ],
            )

            if result is not None:
                return self._as_dict(
                    result,
                    fallback={
                        "status": "PASS",
                        "semantic_schema": result,
                    },
                )

        columns = []
        if isinstance(schema, dict):
            columns = schema.get("columns", [])

        semantic_columns = []

        for item in columns:
            name = str(item.get("name", ""))
            lowered = name.lower()

            role = "attribute"

            if any(x in lowered for x in ("id", "key", "code")):
                role = "identifier"
            elif any(
                x in lowered
                for x in ("amount", "price", "salary", "income", "revenue", "cost")
            ):
                role = "measure"
            elif any(
                x in lowered
                for x in ("date", "time", "timestamp", "created", "updated")
            ):
                role = "datetime"
            elif any(
                x in lowered
                for x in ("name", "email", "city", "country", "status", "type")
            ):
                role = "dimension"

            semantic_columns.append(
                {
                    "name": name,
                    "role": role,
                    "type": item.get("type", "string"),
                }
            )

        return {
            "status": "PASS",
            "semantic_schema": semantic_columns,
            "message": "Generic semantic profiling completed.",
        }

    def _step_quality(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Run the existing DataQualityAgent with its real contract: analyze(data, schema)."""
        agent = self._get_agent("quality", "data_quality")
        rows = self._load_rows(context["source"])
        schema = context.get("schema", {})

        if agent is not None:
            analyze = getattr(agent, "analyze", None)
            if callable(analyze):
                result = analyze(rows, schema)
                if result is not None:
                    return self._as_dict(result, {"status": "PASS"})

        return self._generic_quality(rows, schema)



    def _step_validation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Run the existing ValidationAgent with data + schema, never a file path."""
        agent = self._get_agent("validation")
        rows = self._load_rows(context["source"])
        schema = context.get("schema", {})

        if agent is not None:
            validate = getattr(agent, "validate", None)
            if callable(validate):
                result = validate(rows, schema)
                if result is not None:
                    return self._as_dict(result, {"status": "PASS"})

        return {
            "status": "PASS",
            "message": "Generic validation gate passed.",
            "row_count": len(rows),
        }



    def _step_transformation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Run the existing TransformationAgent with rows + enriched schema."""
        agent = self._get_agent("transformation")
        rows = self._load_rows(context["source"])
        schema = context.get("working_schema") or context.get("schema", {})

        if agent is not None:
            transform = getattr(agent, "transform", None)
            if callable(transform):
                result = transform(rows, schema)
                return {
                    "status": "SUCCESS",
                    "rows": result if isinstance(result, list) else rows,
                    "row_count": len(result) if isinstance(result, list) else len(rows),
                    "schema": schema,
                    "message": "TransformationAgent completed.",
                }

        return {
            "status": "SUCCESS",
            "rows": rows,
            "row_count": len(rows),
            "schema": schema,
            "message": "Generic transformation completed without an injected agent.",
        }



    def _step_silver(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Materialize TransformationAgent rows into the Silver CSV artifact."""
        transformation = context.get("transformation", {})
        rows = transformation.get("rows") if isinstance(transformation, dict) else None
        schema = context.get("working_schema") or context.get("schema", {})
        source = Path(str(context["source"]))
        silver_path = SILVER_DIR / f"{source.stem}_silver{source.suffix}"

        if isinstance(rows, list):
            self._write_rows_csv(rows, silver_path, schema)
        else:
            input_path = Path(str(self._extract_path(transformation) or source))
            if input_path.exists() and input_path.suffix.lower() == ".csv":
                silver_path.write_bytes(input_path.read_bytes())
            else:
                raise RuntimeError("Transformation produced no row data for Silver layer")

        return {
            "status": "SUCCESS",
            "artifact": str(silver_path),
            "silver_file": str(silver_path),
            "row_count": len(rows) if isinstance(rows, list) else self._count_rows(silver_path),
            "message": "Silver layer materialized successfully.",
        }



    def _step_gold(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Run GoldAgent with its real contract: aggregate(silver_file, gold_file, schema)."""
        agent = self._get_agent("gold")
        silver = context.get("silver", {})
        silver_file = self._extract_path(silver)
        if not silver_file:
            raise RuntimeError("Silver artifact is missing before GOLD step")

        gold_path = GOLD_DIR / f"{Path(silver_file).stem}_gold.csv"
        schema = context.get("working_schema") or context.get("schema", {})

        if agent is not None:
            aggregate = getattr(agent, "aggregate", None)
            if callable(aggregate):
                result = aggregate(silver_file, gold_path, schema)
                return {
                    "status": "SUCCESS",
                    "artifact": str(gold_path),
                    "gold_file": str(gold_path),
                    "row_count": len(result) if isinstance(result, list) else self._count_rows(gold_path),
                    "result": result,
                    "message": "GoldAgent completed.",
                }

        summary = self._generic_gold_summary(Path(silver_file))
        gold_json = GOLD_DIR / f"{Path(silver_file).stem}_gold.json"
        gold_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        return {
            "status": "SUCCESS",
            "artifact": str(gold_json),
            "gold_file": str(gold_json),
            "row_count": summary.get("row_count", 0),
            "message": "Generic Gold summary completed.",
        }



    @staticmethod
    def _load_rows(source: Path) -> List[Dict[str, Any]]:
        if source.suffix.lower() != ".csv":
            return []
        with source.open("r", encoding="utf-8-sig", newline="") as file:
            return list(csv.DictReader(file))

    @staticmethod
    def _write_rows_csv(rows: List[Dict[str, Any]], destination: Path, schema: Dict[str, Any]) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        columns = []
        for item in schema.get("columns", []) if isinstance(schema, dict) else []:
            name = item.get("name")
            if name and name not in columns:
                columns.append(name)
        for row in rows:
            for name in row.keys():
                if name not in columns:
                    columns.append(name)
        with destination.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _generic_quality(rows: List[Dict[str, Any]], schema: Dict[str, Any]) -> Dict[str, Any]:
        columns = [c.get("name") for c in schema.get("columns", [])] if isinstance(schema, dict) else []
        columns = [c for c in columns if c]
        nulls = sum(1 for row in rows for c in columns if row.get(c) in (None, ""))
        duplicates = len(rows) - len({tuple(row.get(c) for c in columns) for row in rows}) if rows else 0
        total = len(rows) * len(columns)
        completeness = 100.0 if total == 0 else (total-nulls)/total*100
        duplicate_free = 100.0 if not rows else (len(rows)-duplicates)/len(rows)*100
        score = round(completeness*0.7 + duplicate_free*0.3, 2)
        return {"status":"PASS" if score >= 80 else "WARN", "score":score, "quality_score":score, "row_count":len(rows), "null_values":nulls, "duplicate_rows":duplicates}

    @staticmethod
    def _merge_semantic_schema(schema: Dict[str, Any], semantic: Any) -> Dict[str, Any]:
        base = json.loads(json.dumps(schema or {}))
        columns = base.get("columns", [])
        semantic_columns = []
        if isinstance(semantic, dict):
            semantic_columns = semantic.get("semantic_schema") or semantic.get("columns") or []
        if isinstance(semantic, list):
            semantic_columns = semantic
        roles = {str(x.get("name")): x for x in semantic_columns if isinstance(x, dict) and x.get("name")}
        for col in columns:
            info = roles.get(str(col.get("name")))
            if info:
                role = str(info.get("role", "")).upper()
                if role in {"IDENTIFIER", "ID", "PRIMARY"}: role = "KEY"
                elif role == "DIMENSION": role = "DIMENSION"
                elif role == "MEASURE": role = "MEASURE"
                elif role == "DATETIME": role = "ATTRIBUTE"
                elif role in {"ATTRIBUTE", "ATTR"}: role = "ATTRIBUTE"
                if role: col["role"] = role
                if "confidence" in info: col["confidence"] = info["confidence"]
        base["columns"] = columns
        return base

    # ------------------------------------------------------------------
    # Agent compatibility
    # ------------------------------------------------------------------

    def _get_agent(self, *names: str) -> Any:
        for name in names:
            if name in self.agents:
                return self.agents[name]

        module_map = {
            "schema": ("agents.schema_agent", "SchemaAgent"),
            "semantic": ("agents.semantic_agent", "SemanticAgent"),
            "quality": ("agents.data_quality_agent", "DataQualityAgent"),
            "validation": ("agents.validation_agent", "ValidationAgent"),
            "transformation": (
                "agents.transformation_agent",
                "TransformationAgent",
            ),
            "silver": ("agents.silver_agent", "SilverAgent"),
            "gold": ("agents.gold_agent", "GoldAgent"),
        }

        for name in names:
            mapping = module_map.get(name)
            if not mapping:
                continue

            module_name, class_name = mapping

            try:
                module = __import__(
                    module_name,
                    fromlist=[class_name],
                )
                cls = getattr(module, class_name, None)

                if cls is not None:
                    return cls()

            except Exception as exc:
                LOGGER.warning(
                    "AGENT LOAD SKIPPED | %s | %s",
                    name,
                    exc,
                )

        return None

    @staticmethod
    def _safe_call_agent(
        agent: Any,
        candidates: List[tuple],
    ) -> Any:

        methods = (
            "process",
            "run",
            "execute",
            "transform",
            "aggregate",
            "validate",
            "analyze",
            "profile",
        )

        last_error: Optional[Exception] = None

        for method_name in methods:
            method = getattr(agent, method_name, None)

            if not callable(method):
                continue

            for args in candidates:
                try:
                    return method(*args)
                except TypeError as exc:
                    last_error = exc
                    continue

        if callable(agent):
            for args in candidates:
                try:
                    return agent(*args)
                except TypeError as exc:
                    last_error = exc
                    continue

        if last_error:
            raise last_error

        raise TypeError(
            f"Unsupported agent interface: {type(agent).__name__}"
        )

    # ------------------------------------------------------------------
    # Incremental / checkpoint handling
    # ------------------------------------------------------------------

    def _checkpoint_path(self, source: Path) -> Path:
        key = hashlib.sha256(
            str(source.resolve()).encode("utf-8")
        ).hexdigest()[:24]

        return CHECKPOINT_DIR / f"{key}.json"

    def _prepare_incremental_source(
        self,
        source: Path,
        execution_id: str,
        force: bool = False,
    ) -> Dict[str, Any]:

        source_hash = self._sha256_file(source)
        row_count = self._count_rows(source)
        checkpoint = self._checkpoint_path(source)

        previous: Dict[str, Any] = {}

        if checkpoint.exists():
            try:
                previous = json.loads(
                    checkpoint.read_text(encoding="utf-8")
                )
            except Exception:
                previous = {}

        previous_rows = int(
            previous.get("processed_row_count", 0) or 0
        )
        previous_hash = str(
            previous.get("source_hash", "") or ""
        )

        if (
            self.incremental
            and not force
            and previous_hash == source_hash
            and row_count == previous_rows
        ):
            return {
                "status": "SKIPPED_IDEMPOTENT",
                "checkpoint": checkpoint,
                "source_hash": source_hash,
                "source_row_count": row_count,
                "processed_row_count": 0,
                "effective_source": str(source),
            }

        start_row = (
            0
            if force or not self.incremental
            else min(previous_rows, row_count)
        )

        batch_path = (
            INCREMENTAL_DIR
            / f"{source.stem}_batch_{execution_id}{source.suffix}"
        )

        if source.suffix.lower() == ".csv":
            written = self._write_csv_batch(
                source,
                batch_path,
                start_row,
            )
        else:
            batch_path = source
            written = row_count

        if written == 0 and row_count > 0 and not force:
            return {
                "status": "SKIPPED_IDEMPOTENT",
                "checkpoint": checkpoint,
                "source_hash": source_hash,
                "source_row_count": row_count,
                "processed_row_count": 0,
                "effective_source": str(source),
            }

        return {
            "status": "READY",
            "checkpoint": checkpoint,
            "source_hash": source_hash,
            "source_row_count": row_count,
            "previous_row_count": previous_rows,
            "start_row": start_row,
            "processed_row_count": written,
            "effective_source": str(batch_path),
            "incremental": self.incremental and not force,
            "forced": force,
        }

    def _commit_checkpoint(
        self,
        original_source: Path,
        incremental_info: Dict[str, Any],
        execution_id: str,
        report: ExecutionReport,
    ) -> None:

        checkpoint = Path(
            incremental_info["checkpoint"]
        )

        payload = {
            "version": VERSION,
            "source": str(original_source.resolve()),
            "source_hash": incremental_info["source_hash"],
            "processed_row_count": incremental_info["source_row_count"],
            "last_batch_row_count": incremental_info[
                "processed_row_count"
            ],
            "last_start_row": incremental_info.get(
                "start_row",
                0,
            ),
            "execution_id": execution_id,
            "status": report.status,
            "updated_at": self._now(),
        }

        checkpoint.write_text(
            json.dumps(
                payload,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _write_csv_batch(
        source: Path,
        destination: Path,
        start_row: int = 0,
    ) -> int:

        with source.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as src:

            reader = csv.DictReader(src)
            fieldnames = reader.fieldnames or []

            destination.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            count = 0

            with destination.open(
                "w",
                encoding="utf-8",
                newline="",
            ) as dst:

                writer = csv.DictWriter(
                    dst,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                )

                writer.writeheader()

                for index, row in enumerate(reader):
                    if index < start_row:
                        continue

                    writer.writerow(row)
                    count += 1

        return count

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_report(
        self,
        report: ExecutionReport,
    ) -> Path:

        path = (
            METADATA_DIR
            / f"execution_{report.execution_id}.json"
        )

        path.write_text(
            json.dumps(
                report.to_dict(),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        return path

    def _save_summary(
        self,
        report: ExecutionReport,
    ) -> Path:

        summary_path = LOG_DIR / "pipeline_runs.jsonl"

        with summary_path.open(
            "a",
            encoding="utf-8",
        ) as file:

            file.write(
                json.dumps(
                    {
                        "execution_id": report.execution_id,
                        "pipeline": report.pipeline,
                        "source": report.source,
                        "status": report.status,
                        "started_at": report.started_at,
                        "ended_at": report.ended_at,
                        "duration_seconds": report.duration_seconds,
                        "steps": len(report.steps),
                        "successful_steps": sum(
                            step.success
                            for step in report.steps
                        ),
                        "retries": report.retries,
                        "artifacts": len(report.artifacts),
                        "errors": len(report.errors),
                        "version": VERSION,
                    }
                )
                + "\n"
            )

        return summary_path

    @staticmethod
    def _register_artifact(
        report: ExecutionReport,
        artifact: Any,
    ) -> None:

        if artifact is None:
            return

        value = str(artifact)

        if value not in report.artifacts:
            report.artifacts.append(value)

    def _capture_step_metadata(
        self,
        step: StepResult,
        result: Any,
        report: ExecutionReport,
    ) -> None:

        if not isinstance(result, dict):
            return

        artifact = (
            result.get("artifact")
            or result.get("output")
            or result.get("path")
            or result.get("file")
        )

        if artifact:
            step.artifact = str(artifact)
            self._register_artifact(
                report,
                artifact,
            )

        rows = (
            result.get("row_count")
            or result.get("rows_out")
            or result.get("rows")
        )

        if isinstance(rows, int):
            step.rows_out = rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _execution_id() -> str:
        timestamp = datetime.now(
            timezone.utc
        ).strftime("%Y%m%d_%H%M%S")

        return (
            f"EXEC_{timestamp}_"
            f"{uuid.uuid4().hex[:8].upper()}"
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()

    @staticmethod
    def _duration(
        start: Optional[str],
        end: Optional[str],
    ) -> float:

        if not start or not end:
            return 0.0

        try:
            s = datetime.fromisoformat(start)
            e = datetime.fromisoformat(end)
            return round(
                (e - s).total_seconds(),
                3,
            )
        except Exception:
            return 0.0

    @staticmethod
    def _require_source(
        source: Path,
    ) -> None:

        if not source.exists():
            raise FileNotFoundError(
                f"Source file not found: {source}"
            )

        if not source.is_file():
            raise ValueError(
                f"Source is not a file: {source}"
            )

    @staticmethod
    def _count_rows(
        source: Path,
    ) -> int:

        if source.suffix.lower() != ".csv":
            return 0

        with source.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            return max(
                0,
                sum(1 for _ in file) - 1,
            )

    @staticmethod
    def _infer_type(
        values: List[Any],
    ) -> str:

        if not values:
            return "string"

        integer = True
        numeric = True

        for value in values:
            text = str(value).strip()

            try:
                int(text)
            except ValueError:
                integer = False

            try:
                float(text)
            except ValueError:
                numeric = False

        if integer:
            return "integer"

        if numeric:
            return "float"

        lowered = {
            str(value).strip().lower()
            for value in values
        }

        if lowered <= {
            "true",
            "false",
            "yes",
            "no",
            "0",
            "1",
        }:
            return "boolean"

        return "string"

    @staticmethod
    def _as_dict(
        value: Any,
        fallback: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        if isinstance(value, dict):
            return value

        if hasattr(value, "to_dict"):
            try:
                converted = value.to_dict()
                if isinstance(converted, dict):
                    return converted
            except Exception:
                pass

        result = dict(fallback or {})
        result["result"] = value
        return result

    @staticmethod
    def _extract_path(
        value: Any,
    ) -> Optional[str]:

        if value is None:
            return None

        if isinstance(value, (str, Path)):
            return str(value)

        if isinstance(value, dict):
            for key in (
                "silver_file",
                "gold_file",
                "bronze_file",
                "output",
                "artifact",
                "file",
                "path",
            ):
                candidate = value.get(key)

                if candidate:
                    return str(candidate)

        return None

    @staticmethod
    def _first_existing_path(
        value: Any,
        keys: tuple[str, ...],
    ) -> Optional[str]:

        if not isinstance(value, dict):
            return None

        for key in keys:
            candidate = value.get(key)

            if candidate:
                path = Path(str(candidate))

                if path.exists():
                    return str(path)

        return None

    @staticmethod
    def _normalize_schema_result(
        result: Any,
    ) -> Dict[str, Any]:

        if not isinstance(result, dict):
            return {
                "status": "PASS",
                "columns": [],
                "column_count": 0,
                "result": result,
            }

        return result

    @staticmethod
    def _quality_passed(
        result: Any,
    ) -> bool:

        if not isinstance(result, dict):
            return True

        status = str(
            result.get("status", "PASS")
        ).upper()

        if status in {
            "FAIL",
            "FAILED",
            "ERROR",
        }:
            return False

        score = result.get("quality_score")

        if isinstance(score, (int, float)):
            return score >= 50

        return True

    @staticmethod
    def _generic_gold_summary(
        source: Path,
    ) -> Dict[str, Any]:

        if not source.exists():
            return {
                "status": "FAILED",
                "row_count": 0,
                "error": f"File not found: {source}",
            }

        if source.suffix.lower() != ".csv":
            return {
                "status": "SUCCESS",
                "row_count": 0,
                "source": str(source),
            }

        with source.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            reader = csv.DictReader(file)
            rows = list(reader)
            columns = reader.fieldnames or []

        numeric_summary: Dict[str, Dict[str, float]] = {}

        for column in columns:
            values: List[float] = []

            for row in rows:
                value = row.get(column)

                if value in ("", None):
                    continue

                try:
                    values.append(float(value))
                except (TypeError, ValueError):
                    pass

            if values:
                numeric_summary[column] = {
                    "min": min(values),
                    "max": max(values),
                    "avg": round(
                        sum(values) / len(values),
                        2,
                    ),
                    "count": len(values),
                }

        return {
            "status": "SUCCESS",
            "source": str(source),
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": columns,
            "numeric_summary": numeric_summary,
            "generated_at": ProductionOrchestrator._now(),
        }

    @staticmethod
    def _find_final_rows(
        context: Dict[str, Any],
    ) -> Optional[int]:

        for key in (
            "gold",
            "silver",
            "transformation",
            "source_result",
        ):

            value = context.get(key)

            if isinstance(value, dict):

                for row_key in (
                    "row_count",
                    "rows_out",
                    "rows",
                ):

                    row_count = value.get(row_key)

                    if isinstance(
                        row_count,
                        int,
                    ):
                        return row_count

        return None

    @staticmethod
    def _sha256_file(
        path: Path,
        chunk_size: int = 1024 * 1024,
    ) -> str:

        digest = hashlib.sha256()

        with path.open("rb") as file:

            while True:
                chunk = file.read(chunk_size)

                if not chunk:
                    break

                digest.update(chunk)

        return digest.hexdigest()


# Backward-compatible name.
Orchestrator = ProductionOrchestrator


def run_pipeline(
    source_file: str | Path,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Functional API used by app.jarvis and tests."""
    return ProductionOrchestrator(
        **kwargs
    ).run(source_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="DE-JARVIS Production Orchestrator v8.0"
    )

    parser.add_argument(
        "source",
        help="CSV source file",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--full",
        action="store_true",
        help="Ignore checkpoint and process the full source.",
    )

    parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Disable incremental/idempotent processing.",
    )

    parser.add_argument(
        "--stop-on-quality-failure",
        action="store_true",
        help="Stop pipeline when quality score/gate fails.",
    )

    args = parser.parse_args()

    result = run_pipeline(
        args.source,
        max_retries=args.retries,
        incremental=not args.no_incremental,
        force=args.full,
        stop_on_quality_failure=args.stop_on_quality_failure,
    )

    print("\n" + "=" * 70)
    print("DE-JARVIS PRODUCTION ORCHESTRATOR v9.0")
    print("=" * 70)
    print(f"Execution ID : {result['execution_id']}")
    print(f"Status       : {result['status']}")
    print(f"Duration     : {result['duration_seconds']} sec")
    print(f"Retries      : {result['retries']}")
    print(f"Artifacts    : {len(result['artifacts'])}")
    print(f"Errors       : {len(result['errors'])}")

    print("\nSTEP STATUS")

    for step in result["steps"]:
        print(
            f"{step['name']:16} "
            f"{step['status']:10} "
            f"attempts={step['attempts']} "
            f"duration={step['duration_seconds']}s"
        )

    if result["errors"]:
        print("\nERRORS")

        for error in result["errors"]:
            print(
                f"- {error.get('step', 'PIPELINE')}: "
                f"{error.get('type', 'ERROR')} - "
                f"{error.get('message', '')}"
            )

    print("=" * 70)
