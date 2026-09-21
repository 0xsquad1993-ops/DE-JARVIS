import csv
from pathlib import Path


class TransformationAgent:
    """
    DE-JARVIS Transformation Intelligence Agent v2.9

    Uses semantic roles to automatically generate
    and execute transformation rules.
    """

    VERSION = "2.9"

    def __init__(self):
        self.rules = []

    # =========================================================
    # MAIN TRANSFORMATION
    # =========================================================

    def transform(self, data, schema):

        print(
            "TRANSFORMATION AGENT: "
            "Analyzing transformation requirements..."
        )

        self.rules = []

        columns = schema.get(
            "columns",
            []
        )

        # -----------------------------------------------------
        # BUILD RULES
        # -----------------------------------------------------

        for column in columns:

            name = column.get(
                "name",
                ""
            )

            data_type = column.get(
                "type",
                "string"
            )

            role = column.get(
                "role",
                "ATTRIBUTE"
            )

            confidence = column.get(
                "confidence",
                0.0
            )

            column_rules = self._build_rules(
                name=name,
                data_type=data_type,
                role=role,
                confidence=confidence
            )

            self.rules.append({
                "column": name,
                "role": role,
                "confidence": confidence,
                "rules": column_rules
            })

        # -----------------------------------------------------
        # DISPLAY RULES
        # -----------------------------------------------------

        print()
        print("TRANSFORMATION RULES")
        print("-" * 70)

        for rule_group in self.rules:

            print(
                f"Column      : "
                f"{rule_group['column']}"
            )

            print(
                f"Role        : "
                f"{rule_group['role']}"
            )

            print(
                f"Confidence  : "
                f"{rule_group['confidence']:.2f}"
            )

            for rule in rule_group["rules"]:

                print(
                    f"Rule        : "
                    f"{rule}"
                )

            print("-" * 70)

        # -----------------------------------------------------
        # EXECUTE
        # -----------------------------------------------------

        transformed_data = []

        for row in data:

            transformed_row = {}

            for column in columns:

                name = column.get(
                    "name",
                    ""
                )

                value = row.get(
                    name
                )

                role = column.get(
                    "role",
                    "ATTRIBUTE"
                )

                data_type = column.get(
                    "type",
                    "string"
                )

                transformed_value = (
                    self._transform_value(
                        value=value,
                        role=role,
                        data_type=data_type,
                        column_name=name
                    )
                )

                transformed_row[name] = (
                    transformed_value
                )

            transformed_data.append(
                transformed_row
            )

        print(
            "TRANSFORMATION AGENT: "
            f"{len(transformed_data)} "
            "rows transformed"
        )

        return transformed_data

    # =========================================================
    # RULE ENGINE
    # =========================================================

    def _build_rules(
        self,
        name,
        data_type,
        role,
        confidence
    ):

        rules = []

        # -----------------------------------------------------
        # KEY
        # -----------------------------------------------------

        if role == "KEY":

            rules.append(
                "TRIM"
            )

            if data_type == "integer":

                rules.append(
                    "CAST_INTEGER"
                )

            elif data_type in (
                "float",
                "double",
                "decimal"
            ):

                rules.append(
                    "CAST_NUMERIC"
                )

            rules.append(
                "KEY_VALIDATION"
            )

        # -----------------------------------------------------
        # MEASURE
        # -----------------------------------------------------

        elif role == "MEASURE":

            rules.append(
                "TRIM"
            )

            if data_type == "integer":

                rules.append(
                    "CAST_INTEGER"
                )

            elif data_type in (
                "float",
                "double",
                "decimal"
            ):

                rules.append(
                    "CAST_FLOAT"
                )

            rules.append(
                "NUMERIC_VALIDATION"
            )

            rules.append(
                "NEGATIVE_VALUE_CHECK"
            )

        # -----------------------------------------------------
        # DIMENSION
        # -----------------------------------------------------

        elif role == "DIMENSION":

            rules.append(
                "TRIM"
            )

            rules.append(
                "NORMALIZE_TEXT"
            )

            rules.append(
                "EMPTY_VALUE_CHECK"
            )

        # -----------------------------------------------------
        # ATTRIBUTE
        # -----------------------------------------------------

        elif role == "ATTRIBUTE":

            rules.append(
                "TRIM"
            )

            rules.append(
                "NORMALIZE_TEXT"
            )

        # -----------------------------------------------------
        # TIME
        # -----------------------------------------------------

        elif role == "TIME":

            rules.append(
                "TRIM"
            )

            rules.append(
                "DATE_TIME_NORMALIZATION"
            )

        # -----------------------------------------------------
        # FALLBACK
        # -----------------------------------------------------

        else:

            rules.append(
                "TRIM"
            )

        # -----------------------------------------------------
        # LOW CONFIDENCE
        # -----------------------------------------------------

        if confidence < 0.70:

            rules.append(
                "LOW_CONFIDENCE_REVIEW"
            )

        return rules

    # =========================================================
    # VALUE TRANSFORMATION
    # =========================================================

    def _transform_value(
        self,
        value,
        role,
        data_type,
        column_name
    ):

        # -----------------------------------------------------
        # NULL
        # -----------------------------------------------------

        if value is None:

            return None

        # -----------------------------------------------------
        # STRING CLEANING
        # -----------------------------------------------------

        if isinstance(value, str):

            value = value.strip()

        # -----------------------------------------------------
        # KEY
        # -----------------------------------------------------

        if role == "KEY":

            if data_type == "integer":

                try:

                    return int(value)

                except (
                    ValueError,
                    TypeError
                ):

                    return None

            if data_type in (
                "float",
                "double",
                "decimal"
            ):

                try:

                    return float(value)

                except (
                    ValueError,
                    TypeError
                ):

                    return None

            return value

        # -----------------------------------------------------
        # MEASURE
        # -----------------------------------------------------

        if role == "MEASURE":

            if data_type == "integer":

                try:

                    return int(
                        float(value)
                    )

                except (
                    ValueError,
                    TypeError
                ):

                    return None

            if data_type in (
                "float",
                "double",
                "decimal"
            ):

                try:

                    return float(value)

                except (
                    ValueError,
                    TypeError
                ):

                    return None

            return value

        # -----------------------------------------------------
        # DIMENSION
        # -----------------------------------------------------

        if role == "DIMENSION":

            if isinstance(value, str):

                return " ".join(
                    value.split()
                )

            return value

        # -----------------------------------------------------
        # ATTRIBUTE
        # -----------------------------------------------------

        if role == "ATTRIBUTE":

            if isinstance(value, str):

                return " ".join(
                    value.split()
                )

            return value

        # -----------------------------------------------------
        # TIME
        # -----------------------------------------------------

        if role == "TIME":

            if isinstance(value, str):

                return value.strip()

            return value

        return value

    # =========================================================
    # SAVE
    # =========================================================

    def save(
        self,
        data,
        output_path
    ):

        print(
            "TRANSFORMATION AGENT: "
            "Saving transformed data..."
        )

        output_file = Path(
            output_path
        )

        output_file.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        if not data:

            print(
                "TRANSFORMATION AGENT: "
                "No data to save."
            )

            return

        fieldnames = list(
            data[0].keys()
        )

        with open(
            output_file,
            "w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            writer.writeheader()

            writer.writerows(
                data
            )

        print(
            "TRANSFORMATION AGENT: "
            f"Saved to {output_path}"
        )

    # =========================================================
    # RULE REPORT
    # =========================================================

    def get_rules(self):

        return self.rules