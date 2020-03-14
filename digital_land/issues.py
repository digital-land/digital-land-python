import csv

fieldnames = ["row-number", "field", "issue-type", "value"]


class Issues:
    row_number = 0
    fieldname = "unknown"
    rows = []

    def write(self, row):
        self.rows.append(row)

    def log(self, issue_type, value):
        self.write(
            {
                "field": self.fieldname,
                "issue-type": issue_type,
                "value": value,
                "row-number": self.row_number,
            }
        )


class IssuesFile:
    def __init__(self, f=None, path=None):
        if not f:
            f = open(path, "w", newline="")
        self.writer = csv.DictWriter(f)
        self.writer.writeheader()

    def write(self, row):
        self.writer.writerow(row)
