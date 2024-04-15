import csv


def check_for_duplicate_references(csv_path):
    duplicates = {}
    issues = []
    with csv_path.open(newline="") as csvfile:
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
                    "scope": "row-group",
                    "message": f"Duplicate reference '{ref}' found on rows: {', '.join(map(str, rows))}",
                    "dataset": "dataset",
                    "table_name": "resource",
                    "rows": rows,
                    "row_id": str(rows[0]),
                    "organisation": "organisation",
                }
            )

    return True, "Checked for duplicate references.", issues


def validate_references(csv_path):
    issues = []
    with csv_path.open(newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader, start=1):
            ref = row.get("reference")
            if not ref:  # This will be True for both None and empty strings
                issues.append(
                    {
                        "scope": "value",
                        "message": f"Reference is missing on row {row_number}.",
                        "dataset": "dataset",
                        "table_name": "resource",
                        "field_name": "reference",
                        "row_id": str(row_number),
                        "value": "Missing",
                        "organisation": "organisation",
                    }
                )

    return len(issues) == 0, "Checked for unpopulated references.", issues
