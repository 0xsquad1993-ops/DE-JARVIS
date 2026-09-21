from pathlib import Path
import csv


def find_csv_files(directory="data/raw"):
    """
    Find all CSV files in the given directory.
    """

    folder = Path(directory)

    if not folder.exists():
        raise FileNotFoundError(
            f"Directory not found: {directory}"
        )

    return sorted(folder.glob("*.csv"))


def read_csv(file_path):
    """
    Read a CSV file and return a list of dictionaries.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {file_path}"
        )

    if path.suffix.lower() != ".csv":
        raise ValueError(
            f"Expected CSV file, got: {path.suffix}"
        )

    with path.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(
                f"CSV has no header: {file_path}"
            )

        return list(reader)


def write_csv(data, output_path):
    """
    Write a list of dictionaries to a CSV file.
    """

    if not data:
        raise ValueError(
            "Cannot write empty dataset."
        )

    path = Path(output_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    fieldnames = list(data[0].keys())

    with path.open(
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(data)

    return str(path)


def resolve_csv_file(
    file_name,
    directory="data/raw"
):
    """
    Resolve a CSV file name inside the JARVIS raw-data directory.

    Example:
        employees.csv
        products.csv
    """

    if not file_name:
        raise ValueError(
            "CSV file name cannot be empty."
        )

    folder = Path(directory)

    if not folder.exists():
        raise FileNotFoundError(
            f"Directory not found: {directory}"
        )

    requested_name = Path(file_name).name

    if not requested_name.lower().endswith(".csv"):
        requested_name += ".csv"

    exact_path = folder / requested_name

    if exact_path.exists():
        return str(exact_path)

    # Case-insensitive fallback
    for csv_file in folder.glob("*.csv"):
        if csv_file.name.lower() == requested_name.lower():
            return str(csv_file)

    raise FileNotFoundError(
        f"CSV file not found in {directory}: {file_name}"
    )