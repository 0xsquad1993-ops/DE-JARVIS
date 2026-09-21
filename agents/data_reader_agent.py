import csv
from pathlib import Path


class DataReaderAgent:

    def read_csv(self, file_path):

        print("DATA READER AGENT: Reading data...")

        with open(file_path, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            rows = list(reader)

        print(f"DATA READER AGENT: {len(rows)} rows loaded")

        return rows


if __name__ == "__main__":

    agent = DataReaderAgent()

    data = agent.read_csv(
        "data/raw/employees.csv"
    )

    print("\nDATA:")
    for row in data:
        print(row)