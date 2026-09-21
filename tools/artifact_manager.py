from pathlib import Path
import json
from datetime import datetime
from typing import Any


class ArtifactManager:
    """
    JARVIS Artifact Manager
    Big-data safe metadata/artifact storage.

    VERSION 1.1
    """

    VERSION = "1.1"

    # Large dataset protection
    MAX_LIST_ITEMS = 20
    MAX_DICT_KEYS = 100
    SAMPLE_ROWS = 5

    CATEGORIES = {
        "schema": "schema",
        "semantic": "semantic",
        "quality": "quality",
        "validation": "validation",
        "transformation": "transformation",
        "pipeline": "pipeline",
        "decision": "decision",
        "errors": "errors",
        "command": "command",
        "workflow": "workflow",
        "execution": "execution",
        "gold": "gold",
    }

    def __init__(self, base_dir="data/metadata"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ============================================================
    # DIRECTORY
    # ============================================================

    def ensure_category(self, category):
        category = str(category).strip().lower()

        if category not in self.CATEGORIES:
            raise ValueError(
                f"Unknown artifact category: {category}"
            )

        directory = self.base_dir / self.CATEGORIES[category]
        directory.mkdir(parents=True, exist_ok=True)

        return directory

    # ============================================================
    # BIG DATA SAFE COMPACTION
    # ============================================================

    def compact_for_artifact(
        self,
        value: Any,
        key: str | None = None,
        depth: int = 0,
    ):
        """
        Convert large runtime objects into lightweight metadata.

        Important:
        Actual datasets should NEVER be copied into JSON artifacts.
        """

        # --------------------------------------------------------
        # None / primitive
        # --------------------------------------------------------

        if value is None:
            return None

        if isinstance(value, (str, int, float, bool)):
            return value

        # --------------------------------------------------------
        # Path
        # --------------------------------------------------------

        if isinstance(value, Path):
            return str(value).replace("\\", "/")

        # --------------------------------------------------------
        # Dictionary
        # --------------------------------------------------------

        if isinstance(value, dict):

            # Prevent deeply nested objects
            if depth > 8:
                return {
                    "_artifact_note": "Nested object truncated"
                }

            compacted = {}

            items = list(value.items())

            # Limit extremely large dictionaries
            if len(items) > self.MAX_DICT_KEYS:
                items = items[:self.MAX_DICT_KEYS]

                compacted["_artifact_note"] = (
                    "Dictionary truncated for artifact storage"
                )

            for item_key, item_value in items:

                item_key_str = str(item_key)

                # ------------------------------------------------
                # CRITICAL BIG DATA PROTECTION
                # ------------------------------------------------
                #
                # Any runtime key named "data" may contain
                # hundreds of thousands / millions of rows.
                #
                # Do NOT serialize the complete dataset.
                #
                if item_key_str.lower() in {
                    "data",
                    "records",
                    "rows",
                    "dataset",
                    "raw_data",
                    "transformed_data",
                }:

                    compacted[item_key_str] = (
                        self.compact_data_field(item_value)
                    )

                    continue

                compacted[item_key_str] = (
                    self.compact_for_artifact(
                        item_value,
                        key=item_key_str,
                        depth=depth + 1,
                    )
                )

            return compacted

        # --------------------------------------------------------
        # List / Tuple
        # --------------------------------------------------------

        if isinstance(value, (list, tuple)):

            # Large list
            if len(value) > self.MAX_LIST_ITEMS:

                return {
                    "_artifact_type": "large_list",
                    "total_items": len(value),
                    "stored_items": self.SAMPLE_ROWS,
                    "truncated": True,
                    "sample": [
                        self.compact_for_artifact(
                            item,
                            depth=depth + 1,
                        )
                        for item in value[:self.SAMPLE_ROWS]
                    ],
                }

            return [
                self.compact_for_artifact(
                    item,
                    depth=depth + 1,
                )
                for item in value
            ]

        # --------------------------------------------------------
        # Set
        # --------------------------------------------------------

        if isinstance(value, set):

            values = list(value)

            return {
                "_artifact_type": "set",
                "total_items": len(values),
                "sample": [
                    self.compact_for_artifact(
                        item,
                        depth=depth + 1,
                    )
                    for item in values[:self.SAMPLE_ROWS]
                ],
            }

        # --------------------------------------------------------
        # Fallback
        # --------------------------------------------------------

        return str(value)

    # ============================================================
    # DATA FIELD COMPACTION
    # ============================================================

    def compact_data_field(self, data):
        """
        Special handling for dataset-like fields.

        Example:

        1,000,000 rows
            ↓
        artifact
            ↓
        row_count = 1,000,000
        sample = first 5 rows
        full_data_stored = False
        """

        if data is None:
            return None

        # List dataset
        if isinstance(data, (list, tuple)):

            row_count = len(data)

            return {
                "_artifact_type": "dataset_reference",
                "row_count": row_count,
                "sample_rows": [
                    self.compact_for_artifact(
                        row,
                        depth=1,
                    )
                    for row in data[:self.SAMPLE_ROWS]
                ],
                "full_data_stored": False,
                "reason": (
                    "Full dataset excluded from JSON artifact "
                    "to protect memory and artifact size."
                ),
            }

        # Dictionary dataset-like object
        if isinstance(data, dict):

            return {
                "_artifact_type": "data_object_reference",
                "keys": list(data.keys())[:20],
                "full_data_stored": False,
                "reason": (
                    "Large runtime data object excluded "
                    "from JSON artifact."
                ),
            }

        return {
            "_artifact_type": "data_reference",
            "value": str(data),
            "full_data_stored": False,
        }

    # ============================================================
    # SAVE
    # ============================================================

    def save(
        self,
        category: str,
        file_name: str,
        data: Any,
    ):
        directory = self.ensure_category(category)

        file_name = Path(file_name).stem + ".json"
        output_path = directory / file_name

        # BIG DATA SAFE
        compacted_data = self.compact_for_artifact(data)

        payload = {
            "artifact_version": self.VERSION,
            "category": category,
            "created_at": datetime.now().isoformat(
                timespec="seconds"
            ),
            "data": compacted_data,
        }

        with open(
            output_path,
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                payload,
                file,
                indent=2,
                ensure_ascii=False,
                default=str,
            )

        return str(output_path).replace("\\", "/")

    # ============================================================
    # READ
    # ============================================================

    def read(
        self,
        category: str,
        file_name: str,
    ):

        directory = self.ensure_category(category)

        file_name = Path(file_name).stem + ".json"
        file_path = directory / file_name

        if not file_path.exists():
            raise FileNotFoundError(
                f"Artifact not found: {file_path}"
            )

        with open(
            file_path,
            "r",
            encoding="utf-8",
        ) as file:

            return json.load(file)

    # ============================================================
    # LIST
    # ============================================================

    def list_artifacts(self):

        artifacts = []

        if not self.base_dir.exists():
            return artifacts

        for category_dir in sorted(
            self.base_dir.iterdir()
        ):

            if not category_dir.is_dir():
                continue

            category = category_dir.name

            for file_path in sorted(
                category_dir.glob("*.json")
            ):

                artifacts.append(
                    {
                        "category": category,
                        "name": file_path.name,
                        "path": str(
                            file_path
                        ).replace("\\", "/"),
                        "size": file_path.stat().st_size,
                    }
                )

        return artifacts

    # ============================================================
    # PIPELINE ARTIFACTS
    # ============================================================

    def save_pipeline_artifacts(
        self,
        command,
        workflow,
        execution,
        file_name,
    ):

        stem = Path(file_name).stem

        saved = {}

        # --------------------------------------------------------
        # COMMAND
        # --------------------------------------------------------

        saved["command"] = self.save(
            "command",
            stem,
            command,
        )

        # --------------------------------------------------------
        # WORKFLOW
        # --------------------------------------------------------

        saved["workflow"] = self.save(
            "workflow",
            stem,
            workflow,
        )

        # --------------------------------------------------------
        # EXECUTION
        # --------------------------------------------------------
        #
        # IMPORTANT:
        # save() automatically compacts large "data" fields.
        #
        saved["execution"] = self.save(
            "execution",
            stem,
            execution,
        )

        # --------------------------------------------------------
        # SCHEMA
        # --------------------------------------------------------

        schema = execution.get("schema")

        if schema:
            saved["schema"] = self.save(
                "schema",
                stem,
                schema,
            )

        # --------------------------------------------------------
        # SEMANTIC
        # --------------------------------------------------------

        semantic = execution.get("semantic")

        if semantic:
            saved["semantic"] = self.save(
                "semantic",
                stem,
                semantic,
            )

        # --------------------------------------------------------
        # RESULTS
        # --------------------------------------------------------

        results = execution.get(
            "results",
            {},
        )

        for step_result in results.values():

            if not isinstance(step_result, dict):
                continue

            action = step_result.get("action")
            result = step_result.get("result")

            # ----------------------------------------------------
            # QUALITY
            # ----------------------------------------------------

            if action == "DATA_QUALITY":

                saved["quality"] = self.save(
                    "quality",
                    stem,
                    result,
                )

            # ----------------------------------------------------
            # VALIDATION
            # ----------------------------------------------------

            elif action == "VALIDATE_DATA":

                saved["validation"] = self.save(
                    "validation",
                    stem,
                    result,
                )

            # ----------------------------------------------------
            # TRANSFORMATION
            # ----------------------------------------------------

            elif action == "TRANSFORMATION":

                saved["transformation"] = self.save(
                    "transformation",
                    stem,
                    result,
                )

            # ----------------------------------------------------
            # GOLD
            # ----------------------------------------------------

            elif action == "GOLD_ANALYTICS":

                saved["gold"] = self.save(
                    "gold",
                    f"{stem}_gold",
                    result,
                )

        # --------------------------------------------------------
        # PIPELINE SUMMARY
        # --------------------------------------------------------

        pipeline_summary = {
            "status": execution.get("status"),
            "message": execution.get("message"),
            "completed_steps": execution.get(
                "completed_steps",
                [],
            ),
            "silver_path": execution.get(
                "silver_path"
            ),
            "gold_path": execution.get(
                "gold_path"
            ),
            "current_file": execution.get(
                "current_file"
            ),
            "total_steps": len(
                execution.get(
                    "results",
                    {},
                )
            ),
        }

        saved["pipeline"] = self.save(
            "pipeline",
            stem,
            pipeline_summary,
        )

        return saved


# ================================================================
# DIRECT TEST
# ================================================================

if __name__ == "__main__":

    print()
    print("=" * 70)
    print("JARVIS ARTIFACT MANAGER")
    print("=" * 70)
    print(f"VERSION: {ArtifactManager.VERSION}")
    print()

    manager = ArtifactManager()

    # Small test
    test_data = {
        "status": "SUCCESS",
        "rows": [
            {
                "id": 1,
                "name": "Arun",
            },
            {
                "id": 2,
                "name": "Ravi",
            },
        ],
    }

    output = manager.save(
        "pipeline",
        "test",
        test_data,
    )

    print(f"Created : {output}")

    # Big-data safety test
    large_data = {
        "status": "SUCCESS",
        "data": [
            {
                "id": index,
                "value": index * 10,
            }
            for index in range(100_000)
        ],
    }

    big_output = manager.save(
        "execution",
        "big_data_test",
        large_data,
    )

    print(
        f"Big data artifact : {big_output}"
    )

    print()
    print("ARTIFACTS")
    print("-" * 70)

    for artifact in manager.list_artifacts():
        print(
            f"{artifact['category']:<18}"
            f"{artifact['name']:<30}"
            f"{artifact['size']} bytes"
        )

    print()
    print("BIG DATA SAFETY TEST: PASS")
    print("=" * 70)