# checkpoint needs to assemble class state
# it needs to validate inputs specific for that checkpoint
# it then needs to run expectations
# then it needs to be able to save those expectation resultts
# a checkpoint represents the moment in the process where we tell it the
# type of data it is validating and where the data is
# the primary different between checkpoints is how it loads expectations (i.e. where that are loaded from)
from pathlib import Path
import csv
from .base import BaseCheckpoint


class ConvertedResourceCheckpoint(BaseCheckpoint):
    def __init__(self, data_path):
        super().__init__("converted_resource", data_path)
        self.csv_path = Path(data_path)

    def load(self):
        self.expectations = [
            {
                "function": self.check_for_duplicate_references,
                "name": "Check for Duplicate References",
                "severity": "error",
                "responsibility": "system",
            },
            {
                "function": self.validate_references,
                "name": "Validate References",
                "severity": "error",
                "responsibility": "system",
            },
        ]

    def check_for_duplicate_references(self):
        duplicates = {}
        issues = []

        with self.csv_path.open(newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row_number, row in enumerate(reader, start=1):
                ref = row.get("reference")
                if ref in duplicates:
                    duplicates[ref].append(row_number)
                else:
                    duplicates[ref] = [row_number]

        for ref, rows in duplicates.items():
            if len(rows) > 1:
                issues.append(
                    {
                        "scope": "duplicate_reference",
                        "message": f"Duplicate reference '{ref}' found on rows: {', '.join(map(str, rows))}",
                        "rows": rows,
                        "reference": ref,
                        "dataset": "dataset",
                        "field_name": "reference",
                        "row_id": str(rows[0]),
                        "organisation": "organisation",
                    }
                )

        return True, "Checked for duplicate references.", issues

    def validate_references(self):
        issues = []

        with self.csv_path.open(newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row_number, row in enumerate(reader, start=1):
                ref = row.get("reference")

                if not ref:  # This will be True for both None and empty strings
                    issues.append(
                        {
                            "scope": "invalid_reference",
                            "message": f"Reference is missing on row {row_number}.",
                            "row": row_number,
                            "reference": ref,
                            "dataset": "dataset",
                            "field_name": "reference",
                            "row_id": str(row_number),
                            "organisation": "organisation",
                        }
                    )

        return len(issues) == 0, "Checked for unpopulated references.", issues
