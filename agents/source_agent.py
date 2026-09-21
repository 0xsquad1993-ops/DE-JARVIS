from pathlib import Path


class SourceAgent:

    def scan(self, folder="data/raw"):
        print("\nSOURCE AGENT: Scanning for input data...")

        folder_path = Path(folder)

        if not folder_path.exists():
            print("SOURCE AGENT: Raw data folder not found.")
            return []

        files = list(folder_path.glob("*.csv"))

        if not files:
            print("SOURCE AGENT: No CSV files found.")
            return []

        for file in files:
            print(f"Found: {file.name}")

        return files


if __name__ == "__main__":
    agent = SourceAgent()
    agent.scan()