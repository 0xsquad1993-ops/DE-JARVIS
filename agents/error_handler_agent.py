import os
from datetime import datetime


class ErrorHandlerAgent:

    def __init__(self):
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)

    def log_error(self, agent_name, error):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        log_file = os.path.join(
            self.log_dir,
            "pipeline_errors.log"
        )

        with open(log_file, "a", encoding="utf-8") as file:
            file.write(
                f"[{timestamp}] ERROR | "
                f"Agent: {agent_name} | "
                f"Message: {error}\n"
            )

        print(
            f"ERROR HANDLER: {agent_name} failed"
        )

        print(
            f"ERROR HANDLER: Logged to {log_file}"
        )

    def log_success(self, agent_name, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        log_file = os.path.join(
            self.log_dir,
            "pipeline.log"
        )

        with open(log_file, "a", encoding="utf-8") as file:
            file.write(
                f"[{timestamp}] PASS | "
                f"Agent: {agent_name} | "
                f"{message}\n"
            )

        print(
            f"LOG: {agent_name} - {message}"
        )


if __name__ == "__main__":

    agent = ErrorHandlerAgent()

    agent.log_success(
        "TEST AGENT",
        "Logging system is working"
    )

    agent.log_error(
        "TEST AGENT",
        "Sample error for testing"
    )