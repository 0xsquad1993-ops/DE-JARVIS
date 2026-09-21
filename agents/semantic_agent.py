from collections import Counter


class SemanticAgent:

    VERSION = "2.9"

    # These are treated as true primary/business identifiers.
    PRIMARY_KEY_NAMES = {
        "id",
        "uuid",
        "guid",
        "key",
        "transaction_id",
        "transaction_key",
        "order_id",
        "order_key",
        "invoice_id",
        "invoice_key",
        "record_id",
        "record_key",
        "event_id",
        "event_key",
    }

    # Generic *_id / *_key fields are normally reference identifiers.
    # Example:
    # customer_id -> REFERENCE
    # product_id  -> REFERENCE
    REFERENCE_KEY_PATTERNS = (
        "_id",
        "_key",
        "_uuid",
        "_guid",
    )

    MEASURE_NAMES = {
        "salary",
        "price",
        "amount",
        "revenue",
        "sales",
        "quantity",
        "profit",
        "cost",
        "value",
        "total",
        "sales_amount",
        "unit_price",
        "discount",
        "discount_pct",
    }

    DIMENSION_NAMES = {
        "city",
        "state",
        "country",
        "category",
        "department",
        "region",
        "status",
        "type",
        "payment_method",
        "order_status",
    }

    ATTRIBUTE_NAMES = {
        "name",
        "first_name",
        "last_name",
        "product",
        "description",
        "email",
        "phone",
        "address",
    }

    def analyze(self, schema, data=None):

        print(
            "SEMANTIC AGENT: "
            "Analyzing column meanings..."
        )

        rows = data or []

        schema_columns = schema.get(
            "columns",
            []
        )

        result_columns = []

        for column in schema_columns:

            name = column.get(
                "name",
                ""
            )

            column_type = column.get(
                "type",
                "string"
            )

            nullable = column.get(
                "nullable",
                False
            )

            values = [
                row.get(name)
                for row in rows
                if row.get(name) not in (
                    None,
                    ""
                )
            ]

            cardinality = len(
                set(
                    map(
                        str,
                        values
                    )
                )
            )

            row_count = len(rows)

            unique_ratio = (
                cardinality / row_count
                if row_count
                else 0.0
            )

            null_rate = (
                (row_count - len(values))
                / row_count
                if row_count
                else 0.0
            )

            (
                role,
                confidence,
                reason,
                key_type,
            ) = self._classify(
                name=name,
                column_type=column_type,
                values=values,
                unique_ratio=unique_ratio,
            )

            item = {
                "name": name,
                "type": column_type,
                "nullable": nullable,
                "role": role,
                "confidence": confidence,
                "reason": reason,
                "cardinality": cardinality,
                "unique_ratio": round(
                    unique_ratio,
                    4
                ),
                "null_rate": round(
                    null_rate,
                    4
                ),
            }

            # Keep role = KEY for compatibility
            # with existing ValidationAgent,
            # TransformationAgent and GoldAgent.
            #
            # key_type tells DataQualityAgent
            # whether uniqueness is required.

            if key_type:

                item["key_type"] = key_type

                item["unique_required"] = (
                    key_type == "PRIMARY"
                )

            result_columns.append(item)

            if key_type:

                print(
                    f"SEMANTIC AGENT: "
                    f"{name} -> KEY "
                    f"({key_type}, "
                    f"confidence={confidence:.2f})"
                )

            else:

                print(
                    f"SEMANTIC AGENT: "
                    f"{name} -> {role} "
                    f"(confidence={confidence:.2f})"
                )

        print(
            f"SEMANTIC AGENT: "
            f"{len(result_columns)} "
            f"columns classified"
        )

        return {
            "file": schema.get("file"),
            "row_count": schema.get(
                "row_count",
                len(rows)
            ),
            "column_count": len(
                result_columns
            ),
            "columns": result_columns,
            "version": self.VERSION,
            "semantic_analysis": True,
        }

    def _classify(
        self,
        name,
        column_type,
        values,
        unique_ratio,
    ):

        lower_name = (
            name
            .strip()
            .lower()
        )

        # =====================================================
        # PRIMARY KEY
        # =====================================================

        if lower_name in self.PRIMARY_KEY_NAMES:

            return (
                "KEY",
                0.99,
                "Primary/business identifier; "
                "uniqueness expected",
                "PRIMARY",
            )

        # =====================================================
        # REFERENCE KEY
        # =====================================================

        # Example:
        #
        # customer_id
        # product_id
        # employee_id
        #
        # These can legitimately repeat.

        if (
            lower_name.endswith(
                self.REFERENCE_KEY_PATTERNS
            )
            or lower_name.endswith(
                "_uuid"
            )
            or lower_name.endswith(
                "_guid"
            )
        ):

            return (
                "KEY",
                0.99,
                "Reference identifier; "
                "duplicates are allowed",
                "REFERENCE",
            )

        # =====================================================
        # KNOWN MEASURE
        # =====================================================

        if (
            lower_name in self.MEASURE_NAMES
            and column_type in {
                "integer",
                "float",
                "double",
                "numeric",
            }
        ):

            return (
                "MEASURE",
                0.99,
                "Known numeric business metric",
                None,
            )

        # =====================================================
        # KNOWN DIMENSION
        # =====================================================

        if (
            lower_name in self.DIMENSION_NAMES
            and column_type == "string"
        ):

            return (
                "DIMENSION",
                0.98,
                "Known categorical business "
                "dimension",
                None,
            )

        # =====================================================
        # KNOWN ATTRIBUTE
        # =====================================================

        if lower_name in self.ATTRIBUTE_NAMES:

            return (
                "ATTRIBUTE",
                0.95,
                "Descriptive entity attribute",
                None,
            )

        # =====================================================
        # NUMERIC FALLBACK
        # =====================================================

        if column_type in {
            "integer",
            "float",
            "double",
            "numeric",
        }:

            return (
                "MEASURE",
                0.85,
                "Numeric business measure",
                None,
            )

        # =====================================================
        # HIGH CARDINALITY STRING
        # =====================================================

        if unique_ratio > 0.80:

            return (
                "ATTRIBUTE",
                0.85,
                "High-cardinality "
                "descriptive attribute",
                None,
            )

        # =====================================================
        # LOW CARDINALITY STRING
        # =====================================================

        return (
            "DIMENSION",
            0.80,
            "Low-cardinality "
            "categorical field",
            None,
        )


if __name__ == "__main__":

    print(
        f"SemanticAgent version: "
        f"{SemanticAgent.VERSION}"
    )