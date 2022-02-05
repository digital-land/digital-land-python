import csv

fieldnames = ["dataset", "resource", "line-number", "field", "issue-type", "value"]


class Issues:
    def __init__(self, dataset="", resource=""):
        self.dataset = dataset
        self.resource = resource
        self.rows = []
        self.fieldname = "unknown"
        self.line_number = 0

    def write(self, row):
        self.rows.append(row)

    def log(self, issue_type, value):
        self.log_issue(self.fieldname, issue_type, value)

    def log_issue(self, fieldname, issue_type, value, line_number=0):
        self.write(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "field": fieldname,
                "issue-type": issue_type,
                "value": value,
                "line-number": line_number or self.line_number,
            }
        )


class IssuesFile:
    def __init__(self, f=None, path=None):
        if not f:
            f = open(path, "w", newline="")
        self.writer = csv.DictWriter(f, fieldnames)
        self.writer.writeheader()

    def write(self, row):
        self.writer.writerow(row)

    def write_issues(self, issues):
        for issue in issues.rows:
            self.write(issue)
