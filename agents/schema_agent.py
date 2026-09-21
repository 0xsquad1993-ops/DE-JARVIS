from pathlib import Path
import csv


class SchemaAgent:

    def analyze(self, file_path):
        print("\nSCHEMA AGENT: Analyzing dataset schema...")

        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            columns = reader.fieldnames or []

            rows = []
            for row in reader:
                rows.append(row)

        schema = []

        for column in columns:

            values = [
                row[column]
                for row in rows
                if row.get(column) not in (None, "")
            ]

            detected_type = self.detect_type(values)

            schema.append({
                "name": column,
                "type": detected_type,
                "nullable": len(values) < len(rows)
            })

        result = {
            "file": file_path.name,
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": schema
        }

        print(f"SCHEMA AGENT: {len(columns)} columns detected")
        print(f"SCHEMA AGENT: {len(rows)} rows detected")

        for column in schema:
            print(
                f"  {column['name']} "
                f"-> {column['type']} "
                f"(nullable={column['nullable']})"
            )

        return result

    def detect_type(self, values):

        if not values:
            return "string"

        if all(self.is_integer(value) for value in values):
            return "integer"

        if all(self.is_float(value) for value in values):
            return "float"

        if all(self.is_boolean(value) for value in values):
            return "boolean"

        return "string"

    @staticmethod
    def is_integer(value):

        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_float(value):

        try:
            float(value)
            return "." in str(value)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_boolean(value):

        return str(value).strip().lower() in {
            "true",
            "false",
            "yes",
            "no"
        }


if __name__ == "__main__":

    agent = SchemaAgent()

    result = agent.analyze(
        "data/raw/employees.csv"
    )

    print("\nSCHEMA RESULT")
    print("=" * 60)

    for column in result["columns"]:
        print(column)