from digital_land.phase.phase import Phase
import csv


class PostConversionPhase(Phase):
    def __init__(
        self,
        issues,
    ):
        self.issues = issues

    def process(self, stream):
        self.validate_references(stream.f.name)
        self.check_for_duplicate_references(stream.f.name)
        return stream

    def check_for_duplicate_references(self, csv_path):
        duplicates = {}
        with open(csv_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row_number, row in enumerate(reader, start=1):
                ref = row.get("reference")
                if (
                    ref
                ):  # Don't check None or empty references, as these will be picked up by validate_references
                    if ref in duplicates:
                        duplicates[ref].append(row_number)
                    else:
                        duplicates[ref] = [row_number]

        for ref, rows in duplicates.items():
            if len(rows) > 1:
                self.issues.log_issue(
                    "reference",
                    "duplicate-reference",
                    ref,
                    f"Duplicate reference '{ref}' found on rows: {', '.join(map(str, rows))}",
                )

    def validate_references(self, csv_path):
        with open(csv_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row_number, row in enumerate(reader, start=1):
                ref = row.get("reference")
                if not ref:  # This will be True for both None and empty strings
                    self.issues.log_issue(
                        "reference",
                        "missing-reference",
                        ref,
                        f"Reference missing on row {row_number}",
                        row_number + 1,
                    )
