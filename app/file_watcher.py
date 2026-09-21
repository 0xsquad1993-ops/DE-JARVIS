from pathlib import Path
import time

from app.jarvis import Jarvis


class FileWatcher:
    VERSION = "2.0"

    def __init__(
        self,
        folder="data/raw",
        interval=3
    ):
        self.folder = Path(folder)
        self.interval = interval
        self.known_files = set()

        # JARVIS AI engine
        self.jarvis = Jarvis()

    def scan(self):
        """
        Scan the raw folder for CSV files.
        """

        if not self.folder.exists():
            self.folder.mkdir(
                parents=True,
                exist_ok=True
            )

        return set(
            self.folder.glob("*.csv")
        )

    def process_file(self, file):
        """
        Send newly detected file to JARVIS.
        """

        print()
        print("=" * 70)
        print("🚨 NEW RAW FILE DETECTED")
        print("=" * 70)

        print(f"File       : {file.name}")
        print(f"Full Path  : {file}")
        print()

        command = (
            f"{file.name} full pipeline pannu"
        )

        print("🎙 JARVIS COMMAND")
        print(f"   {command}")
        print()

        print("🧠 JARVIS AI: Processing...")
        print()

        try:
            result = self.jarvis.run(command)

            print()
            print("=" * 70)
            print("✅ JARVIS: AUTOMATIC PIPELINE FINISHED")
            print("=" * 70)

            return result

        except Exception as error:

            print()
            print("=" * 70)
            print("❌ JARVIS: PIPELINE FAILED")
            print("=" * 70)

            print(f"Error: {error}")

            return None

    def start(self):

        print()
        print("=" * 70)
        print("              DE-JARVIS FILE WATCHER")
        print("=" * 70)

        print(
            f"Version         : {self.VERSION}"
        )

        print(
            f"Watching folder : {self.folder}"
        )

        print(
            f"Check interval  : {self.interval} seconds"
        )

        print(
            "AI Engine       : JARVIS v4.0"
        )

        print(
            "Status          : WATCHING"
        )

        print()

        print(
            "Waiting for new CSV files..."
        )

        print(
            "Existing files will be ignored."
        )

        print(
            "Press CTRL+C to stop."
        )

        print("=" * 70)

        # --------------------------------------------------
        # Existing files are ignored
        # --------------------------------------------------

        self.known_files = self.scan()

        print()
        print(
            f"Existing CSV files : {len(self.known_files)}"
        )

        print()
        print(
            "👁 JARVIS FILE WATCHER ACTIVE..."
        )

        # --------------------------------------------------
        # Watch loop
        # --------------------------------------------------

        while True:

            time.sleep(
                self.interval
            )

            current_files = self.scan()

            new_files = (
                current_files
                - self.known_files
            )

            # --------------------------------------------------
            # New file detected
            # --------------------------------------------------

            if new_files:

                for file in sorted(new_files):

                    self.process_file(file)

            # --------------------------------------------------
            # Update known files
            # --------------------------------------------------

            self.known_files = current_files


# ============================================================
# START WATCHER
# ============================================================

if __name__ == "__main__":

    watcher = FileWatcher()

    try:

        watcher.start()

    except KeyboardInterrupt:

        print()
        print("=" * 70)
        print(
            "DE-JARVIS FILE WATCHER: STOPPED"
        )
        print("=" * 70)