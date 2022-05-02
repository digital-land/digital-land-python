import csv
from datetime import datetime


def entry_date():
    return datetime.utcnow().isoformat()[:19] + "Z"


class Log:
    def __init__(self, dataset="", resource=""):
        self.dataset = dataset
        self.resource = resource
        self.rows = []
        self.fieldname = "unknown"
        self.line_number = 0
        self.entry_number = 0

    def add(self, *args, **kwargs):
        pass

    def save(self, path=None, f=None):
        if not f:
            f = open(path, "w", newline="")
        writer = csv.DictWriter(f, self.fieldnames)
        writer.writeheader()
        for row in self.rows:
            writer.writerow(row)


class IssueLog(Log):
    fieldnames = [
        "dataset",
        "resource",
        "line-number",
        "entry-number",
        "field",
        "issue-type",
        "value",
    ]

    def log(self, issue_type, value):
        self.log_issue(self.fieldname, issue_type, value)

    def log_issue(self, fieldname, issue_type, value, line_number=0, entry_number=0):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "field": fieldname,
                "issue-type": issue_type,
                "value": value,
                "line-number": line_number or self.line_number,
                "entry-number": entry_number or self.entry_number,
            }
        )


class ColumnFieldLog(Log):
    fieldnames = ["entry-date", "dataset", "resource", "column", "field"]

    def add(self, column, field):
        self.rows.append(
            {
                "entry-date": entry_date(),
                "dataset": self.dataset,
                "resource": self.resource,
                "column": column,
                "field": field,
            }
        )


class DatasetResourceLog(Log):
    fieldnames = [
        "entry-date",
        "dataset",
        "resource",
        "entry-count",
        "line-count",
        "mime-type",
        "internal-path",
        "internal-mime-type",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entry_count = 0
        self.line_count = 0
        self.mime_type = ""
        self.internal_path = ""
        self.internal_mime_type = ""

    def add(self):
        self.rows.append(
            {
                "entry-date": entry_date(),
                "dataset": self.dataset,
                "resource": self.resource,
                "entry-count": self.entry_count,
                "line-count": self.line_count,
                "mime-type": self.mime_type,
                "internal-path": self.internal_path,
                "internal-mime-type": self.internal_mime_type,
            }
        )

    def save(self, *args, **kwargs):
        self.add()
        super().save(*args, **kwargs)
