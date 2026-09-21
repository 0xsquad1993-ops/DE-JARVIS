from collections import Counter


class DataQualityAgent:

    VERSION = "3.0"

    def analyze(self, data, schema):

        print(
            "DATA QUALITY AGENT: "
            "Running dynamic quality checks..."
        )

        data = data or []
        schema = schema or {}

        columns = schema.get(
            "columns",
            []
        )

        # =====================================================
        # TOTAL CHECKS
        # =====================================================

        total_checks = 7
        failed_checks = 0

        checks = []

        # =====================================================
        # 1. ROW COUNT
        # =====================================================

        row_count = len(data)

        row_status = (
            "PASS"
            if row_count > 0
            else "FAIL"
        )

        checks.append({
            "name": "Row Count",
            "status": row_status,
            "message": (
                f"{row_count} rows detected"
            ),
        })

        if row_count == 0:
            failed_checks += 1

        # =====================================================
        # 2. EMPTY ROWS
        # =====================================================

        empty_rows = sum(
            1
            for row in data
            if (
                not row
                or all(
                    value in (None, "")
                    for value in row.values()
                )
            )
        )

        empty_status = (
            "PASS"
            if empty_rows == 0
            else "FAIL"
        )

        checks.append({
            "name": "Empty Rows",
            "status": empty_status,
            "message": (
                f"{empty_rows} empty rows"
            ),
        })

        if empty_rows:
            failed_checks += 1

        # =====================================================
        # 3. NULL VALUES
        # =====================================================

        null_count = 0

        for column in columns:

            name = column.get("name")

            for row in data:

                if row.get(name) in (
                    None,
                    ""
                ):

                    null_count += 1

        null_status = (
            "PASS"
            if null_count == 0
            else "FAIL"
        )

        checks.append({
            "name": "Null Values",
            "status": null_status,
            "message": (
                f"{null_count} null values"
            ),
        })

        if null_count:
            failed_checks += 1

        # =====================================================
        # 4. DUPLICATE ROWS
        # =====================================================

        duplicate_rows = (
            self._duplicate_row_count(data)
        )

        duplicate_row_status = (
            "PASS"
            if duplicate_rows == 0
            else "FAIL"
        )

        checks.append({
            "name": "Duplicate Rows",
            "status": duplicate_row_status,
            "message": (
                f"{duplicate_rows} "
                f"duplicate rows"
            ),
        })

        if duplicate_rows:
            failed_checks += 1

        # =====================================================
        # 5. PRIMARY KEY UNIQUENESS
        # =====================================================

        # IMPORTANT:
        #
        # PRIMARY KEY:
        #     Must be unique.
        #
        # REFERENCE KEY:
        #     Duplicates are allowed.
        #
        # Example:
        #
        # transaction_id -> PRIMARY
        # customer_id    -> REFERENCE
        #
        # customer_id can appear many times.

        primary_keys = [
            column
            for column in columns
            if (
                column.get("role") == "KEY"
                and (
                    column.get("key_type")
                    == "PRIMARY"
                    or column.get(
                        "unique_required"
                    ) is True
                )
            )
        ]

        # -----------------------------------------------------
        # Legacy schema compatibility
        # -----------------------------------------------------

        if not primary_keys:

            primary_keys = (
                self._infer_primary_keys(
                    data,
                    columns
                )
            )

        duplicate_key_messages = []

        duplicate_key_total = 0

        for column in primary_keys:

            name = column.get(
                "name"
            )

            duplicates = (
                self._duplicate_value_count(
                    data,
                    name
                )
            )

            duplicate_key_total += (
                duplicates
            )

            if duplicates:

                duplicate_key_messages.append(
                    f"{duplicates} duplicate IDs "
                    f"in {name}"
                )

        if duplicate_key_messages:

            key_status = "FAIL"

            key_message = (
                "; ".join(
                    duplicate_key_messages
                )
            )

            failed_checks += 1

        else:

            key_status = "PASS"

            primary_names = [
                column.get("name")
                for column in primary_keys
            ]

            if primary_names:

                key_message = (
                    "0 duplicate IDs in "
                    + ", ".join(
                        primary_names
                    )
                )

            else:

                key_message = (
                    "No unique primary key "
                    "detected"
                )

        checks.append({
            "name": "Duplicate ID",
            "status": key_status,
            "message": key_message,
        })

        # =====================================================
        # 6. DATA TYPES
        # =====================================================

        type_errors = (
            self._type_errors(
                data,
                columns
            )
        )

        type_status = (
            "PASS"
            if type_errors == 0
            else "FAIL"
        )

        checks.append({
            "name": "Data Types",
            "status": type_status,
            "message": (
                f"{type_errors} type errors"
            ),
        })

        if type_errors:
            failed_checks += 1

        # =====================================================
        # 7. COLUMN CONSISTENCY
        # =====================================================

        expected_columns = [
            column.get("name")
            for column in columns
            if column.get("name")
        ]

        inconsistent_rows = sum(
            1
            for row in data
            if set(row.keys())
            != set(expected_columns)
        )

        consistency_status = (
            "PASS"
            if inconsistent_rows == 0
            else "FAIL"
        )

        checks.append({
            "name": "Column Consistency",
            "status": consistency_status,
            "message": (
                f"{inconsistent_rows} "
                f"inconsistent rows"
            ),
        })

        if inconsistent_rows:
            failed_checks += 1

        # =====================================================
        # SCORE
        # =====================================================

        score = (
            (
                (
                    total_checks
                    - failed_checks
                )
                / total_checks
            )
            * 100
            if total_checks
            else 0.0
        )

        status = (
            "PASS"
            if failed_checks == 0
            else "FAIL"
        )

        # =====================================================
        # PRINT REPORT
        # =====================================================

        print()

        print(
            "DATA QUALITY REPORT"
        )

        print(
            "-" * 70
        )

        for check in checks:

            print(
                f"[{check['status']}] "
                f"{check['name']} - "
                f"{check['message']}"
            )

        print(
            "-" * 70
        )

        print(
            f"DATA QUALITY SCORE: "
            f"{score:.2f}%"
        )

        print(
            f"DATA QUALITY STATUS: "
            f"{status}"
        )

        # =====================================================
        # REFERENCE KEY INFORMATION
        # =====================================================

        reference_keys = [
            column.get("name")
            for column in columns
            if (
                column.get("role") == "KEY"
                and column.get("key_type")
                == "REFERENCE"
            )
        ]

        if reference_keys:

            print(
                "REFERENCE KEYS: "
                + ", ".join(
                    reference_keys
                )
                + " | duplicates allowed"
            )

        # =====================================================
        # FINAL RESULT
        # =====================================================

        return {
            "status": status,
            "score": round(
                score,
                2
            ),

            "row_count": row_count,

            "checks": checks,

            "duplicate_rows": (
                duplicate_rows
            ),

            "duplicate_ids": (
                duplicate_key_total
            ),

            "primary_keys": [
                column.get("name")
                for column in primary_keys
            ],

            "reference_keys": (
                reference_keys
            ),

            "null_values": (
                null_count
            ),

            "type_errors": (
                type_errors
            ),

            "inconsistent_rows": (
                inconsistent_rows
            ),

            "failed_checks": (
                failed_checks
            ),

            "total_checks": (
                total_checks
            ),

            "version": self.VERSION,
        }

    # =========================================================
    # DUPLICATE ROW CHECK
    # =========================================================

    @staticmethod
    def _duplicate_row_count(data):

        seen = set()

        duplicates = 0

        for row in data:

            row_key = tuple(
                sorted(
                    (
                        str(key),
                        str(value)
                    )
                    for key, value
                    in row.items()
                )
            )

            if row_key in seen:

                duplicates += 1

            else:

                seen.add(row_key)

        return duplicates

    # =========================================================
    # DUPLICATE KEY VALUE CHECK
    # =========================================================

    @staticmethod
    def _duplicate_value_count(
        data,
        column_name
    ):

        values = [
            str(
                row.get(column_name)
            ).strip()

            for row in data

            if row.get(
                column_name
            ) not in (
                None,
                ""
            )
        ]

        counts = Counter(
            values
        )

        # Example:
        #
        # 1
        # 1
        # 1
        # 2
        #
        # duplicate count = 2
        #
        # Not 3.

        return sum(
            count - 1
            for count in counts.values()
            if count > 1
        )

    # =========================================================
    # LEGACY PRIMARY KEY DETECTION
    # =========================================================

    @staticmethod
    def _infer_primary_keys(
        data,
        columns
    ):

        candidates = []

        # -----------------------------------------------------
        # Strong primary-key names
        # -----------------------------------------------------

        strong_names = {
            "id",
            "uuid",
            "guid",
            "transaction_id",
            "transaction_key",
            "order_id",
            "invoice_id",
            "record_id",
            "event_id",
        }

        for column in columns:

            if column.get(
                "role"
            ) != "KEY":

                continue

            name = str(
                column.get(
                    "name",
                    ""
                )
            ).lower()

            if name in strong_names:

                candidates.append(
                    column
                )

        # -----------------------------------------------------
        # Fallback
        # -----------------------------------------------------

        if not candidates:

            for column in columns:

                if column.get(
                    "role"
                ) != "KEY":

                    continue

                name = column.get(
                    "name"
                )

                values = [
                    row.get(name)

                    for row in data

                    if row.get(name)
                    not in (
                        None,
                        ""
                    )
                ]

                if (
                    values
                    and len(
                        set(
                            map(
                                str,
                                values
                            )
                        )
                    )
                    == len(values)
                ):

                    candidates.append(
                        column
                    )

        return candidates

    # =========================================================
    # DATA TYPE VALIDATION
    # =========================================================

    @staticmethod
    def _type_errors(
        data,
        columns
    ):

        errors = 0

        for column in columns:

            name = column.get(
                "name"
            )

            expected = column.get(
                "type"
            )

            for row in data:

                value = row.get(
                    name
                )

                if value in (
                    None,
                    ""
                ):

                    continue

                # INTEGER
                if expected == "integer":

                    try:

                        int(value)

                    except (
                        ValueError,
                        TypeError
                    ):

                        errors += 1

                # FLOAT / DOUBLE / NUMERIC
                elif expected in {
                    "float",
                    "double",
                    "numeric",
                }:

                    try:

                        float(value)

                    except (
                        ValueError,
                        TypeError
                    ):

                        errors += 1

        return errors


if __name__ == "__main__":

    print(
        f"DataQualityAgent version: "
        f"{DataQualityAgent.VERSION}"
    )