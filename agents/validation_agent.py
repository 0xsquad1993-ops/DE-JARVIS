class ValidationAgent:

    def validate(self, data, schema=None):

        print("VALIDATION AGENT: Running dynamic validation...")

        # --------------------------------------------------
        # BASIC DATA CHECK
        # --------------------------------------------------

        if data is None:

            print(
                "VALIDATION AGENT: "
                "Data is None"
            )

            return False

        if not data:

            print(
                "VALIDATION AGENT: "
                "No data available"
            )

            return False

        # --------------------------------------------------
        # DETECT COLUMNS FROM DATA
        # --------------------------------------------------

        actual_columns = list(data[0].keys())

        print(
            f"VALIDATION AGENT: "
            f"Detected columns: {actual_columns}"
        )

        # --------------------------------------------------
        # SCHEMA VALIDATION
        # --------------------------------------------------

        if schema:

            expected_columns = [
                column["name"]
                for column in schema.get(
                    "columns",
                    []
                )
            ]

            missing_columns = [
                column
                for column in expected_columns
                if column not in actual_columns
            ]

            unexpected_columns = [
                column
                for column in actual_columns
                if column not in expected_columns
            ]

            if missing_columns:

                print(
                    "VALIDATION AGENT: "
                    f"Missing columns: "
                    f"{missing_columns}"
                )

                return False

            if unexpected_columns:

                print(
                    "VALIDATION AGENT: "
                    f"Unexpected columns: "
                    f"{unexpected_columns}"
                )

                return False

            print(
                "VALIDATION AGENT: "
                "Schema columns: PASS"
            )

        # --------------------------------------------------
        # ROW COLUMN CONSISTENCY
        # --------------------------------------------------

        inconsistent_rows = []

        for row_number, row in enumerate(
            data,
            start=1
        ):

            row_columns = list(row.keys())

            if set(row_columns) != set(actual_columns):

                inconsistent_rows.append(
                    row_number
                )

        if inconsistent_rows:

            print(
                "VALIDATION AGENT: "
                f"Inconsistent rows: "
                f"{inconsistent_rows}"
            )

            return False

        print(
            "VALIDATION AGENT: "
            "Column consistency: PASS"
        )

        # --------------------------------------------------
        # NULL VALIDATION
        # --------------------------------------------------

        null_count = 0

        for row in data:

            for column in actual_columns:

                value = row.get(column)

                if value is None:

                    null_count += 1

                elif isinstance(value, str):

                    if value.strip() == "":

                        null_count += 1

        print(
            f"VALIDATION AGENT: "
            f"Null values: {null_count}"
        )

        # --------------------------------------------------
        # DUPLICATE KEY DETECTION
        # --------------------------------------------------

        key_column = self._find_key_column(
            schema,
            actual_columns
        )

        duplicate_count = 0

        if key_column:

            seen = set()

            for row in data:

                value = row.get(key_column)

                if value in seen:

                    duplicate_count += 1

                else:

                    seen.add(value)

            print(
                f"VALIDATION AGENT: "
                f"Key column: {key_column}"
            )

            print(
                f"VALIDATION AGENT: "
                f"Duplicate keys: "
                f"{duplicate_count}"
            )

        else:

            print(
                "VALIDATION AGENT: "
                "No semantic key detected"
            )

        # --------------------------------------------------
        # DATA TYPE VALIDATION
        # --------------------------------------------------

        type_errors = []

        if schema:

            schema_columns = {
                column["name"]: column
                for column in schema.get(
                    "columns",
                    []
                )
            }

            for row_number, row in enumerate(
                data,
                start=1
            ):

                for column_name, column_info in (
                    schema_columns.items()
                ):

                    value = row.get(column_name)

                    if value in (
                        None,
                        ""
                    ):

                        continue

                    expected_type = column_info.get(
                        "type"
                    )

                    if not self._valid_type(
                        value,
                        expected_type
                    ):

                        type_errors.append(
                            (
                                row_number,
                                column_name,
                                value,
                                expected_type
                            )
                        )

        if type_errors:

            print(
                "VALIDATION AGENT: "
                f"Type errors: "
                f"{len(type_errors)}"
            )

            for error in type_errors[:10]:

                print(
                    f"  Row {error[0]} | "
                    f"Column {error[1]} | "
                    f"Value {error[2]} | "
                    f"Expected {error[3]}"
                )

            return False

        print(
            "VALIDATION AGENT: "
            "Data types: PASS"
        )

        # --------------------------------------------------
        # FINAL VALIDATION STATUS
        # --------------------------------------------------

        if duplicate_count > 0:

            print(
                "VALIDATION AGENT: "
                "VALIDATION FAILED - "
                "Duplicate keys detected"
            )

            return False

        print()
        print(
            "VALIDATION AGENT: "
            "DATA VALIDATION: PASS"
        )

        return True

    # ======================================================
    # FIND SEMANTIC KEY
    # ======================================================

    def _find_key_column(
        self,
        schema,
        actual_columns
    ):

        if not schema:

            return None

        for column in schema.get(
            "columns",
            []
        ):

            if column.get("role") == "KEY":

                column_name = column.get(
                    "name"
                )

                if column_name in actual_columns:

                    return column_name

        return None

    # ======================================================
    # DATA TYPE CHECK
    # ======================================================

    @staticmethod
    def _valid_type(
        value,
        expected_type
    ):

        if expected_type == "string":

            return isinstance(
                value,
                str
            )

        if expected_type == "integer":

            try:

                int(value)

                return (
                    str(value).strip()
                    != ""
                )

            except (
                ValueError,
                TypeError
            ):

                return False

        if expected_type == "float":

            try:

                float(value)

                return (
                    str(value).strip()
                    != ""
                )

            except (
                ValueError,
                TypeError
            ):

                return False

        if expected_type == "boolean":

            return str(value).lower() in {
                "true",
                "false",
                "yes",
                "no",
                "1",
                "0"
            }

        return True


# ============================================================
# DIRECT TEST
# ============================================================

if __name__ == "__main__":

    agent = ValidationAgent()

    # --------------------------------------------------------
    # TEST DATA
    # --------------------------------------------------------

    sample_data = [

        {
            "product": "Laptop",
            "category": "Electronics",
            "price": "50000"
        },

        {
            "product": "Phone",
            "category": "Electronics",
            "price": "20000"
        },

        {
            "product": "Tablet",
            "category": "Electronics",
            "price": "30000"
        },

        {
            "product": "Chair",
            "category": "Furniture",
            "price": "8000"
        }

    ]

    sample_schema = {

        "columns": [

            {
                "name": "product",
                "type": "string",
                "nullable": False,
                "role": "DIMENSION"
            },

            {
                "name": "category",
                "type": "string",
                "nullable": False,
                "role": "DIMENSION"
            },

            {
                "name": "price",
                "type": "integer",
                "nullable": False,
                "role": "MEASURE"
            }

        ]

    }

    result = agent.validate(
        sample_data,
        sample_schema
    )

    print()
    print("=" * 60)
    print("VALIDATION RESULT")
    print("=" * 60)
    print(
        f"VALIDATION STATUS: {result}"
    )