import csv


class Log:
    def __init__(self, dataset="", resource=""):
        self.dataset = dataset
        self.resource = resource
        self.rows = []
        self.fieldname = "unknown"
        self.line_number = 0

    def add(self, *args, **kwargs):
        pass

    def save(self, path):
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
        "field",
        "issue-type",
        "value",
    ]

    def log(self, issue_type, value):
        self.log_issue(self.fieldname, issue_type, value)

    def log_issue(self, fieldname, issue_type, value, line_number=0):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "field": fieldname,
                "issue-type": issue_type,
                "value": value,
                "line-number": line_number or self.line_number,
            }
        )


class ColumnFieldLog(Log):
    fieldnames = ["dataset", "resource", "column", "field"]

    def add(self, column, issue_type, fieldname):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "column": column,
                "issue-type": issue_type,
                "field": fieldname,
            }
        )
