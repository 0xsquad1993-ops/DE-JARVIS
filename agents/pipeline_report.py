from datetime import datetime


class PipelineReport:

    def __init__(self):
        self.start_time = datetime.now()
        self.steps = []

    def add_step(self, name, status, details=""):
        self.steps.append({
            "name": name,
            "status": status,
            "details": details
        })

    def show(self):
        print("\n")
        print("=" * 60)
        print("              DE-JARVIS PIPELINE REPORT")
        print("=" * 60)

        print(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        print("\nSTEPS:")

        for step in self.steps:
            status = "PASS" if step["status"] else "FAIL"

            print(
                f"[{status}] "
                f"{step['name']} "
                f"- {step['details']}"
            )

        passed = sum(1 for step in self.steps if step["status"])
        total = len(self.steps)

        print("\n" + "-" * 60)
        print(f"RESULT: {passed}/{total} steps passed")
        print("=" * 60)