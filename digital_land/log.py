import csv
from datetime import datetime
import pandas as pd
import yaml


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
        "message",
    ]

    def log(
        self,
        issue_type,
        value,
        message=None,
    ):
        self.log_issue(self.fieldname, issue_type, value, message)

    def log_issue(
        self, fieldname, issue_type, value, message=None, line_number=0, entry_number=0
    ):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "field": fieldname,
                "issue-type": issue_type,
                "value": value,
                "line-number": line_number or self.line_number,
                "entry-number": entry_number or self.entry_number,
                "message": message,
            }
        )

    def add_severity_column(self, severity_mapping_path):
        # Load only the 'severity' column from severity_mapping
        severity_mapping = pd.read_csv(
            severity_mapping_path,
            usecols=["issue-type", "severity", "description", "responsibility"],
        )

        # Convert the existing log data to a DataFrame
        log_df = pd.DataFrame(self.rows)

        if not log_df.empty:
            # Merge with severity_mapping based on 'issue-type'
            merged_df = pd.merge(
                log_df,
                severity_mapping,
                how="left",
                left_on="issue-type",
                right_on="issue-type",
            )

            # Add the new 'severity' column to the log data
            self.fieldnames.append("severity")
            self.fieldnames.append("description")
            self.fieldnames.append("responsibility")
            self.rows = merged_df.to_dict(orient="records")

    def appendErrorMessage(self, mapping_path):
        # Read the mapping from the JSON config file
        with open(mapping_path, "r") as f:
            mapping_data = yaml.safe_load(f)
        mapping = pd.DataFrame(mapping_data["mappings"])

        # Update the 'description' column based on the mapping data
        for row in self.rows:
            mapping_row = mapping[
                (mapping["field"] == row["field"])
                & (mapping["issue-type"] == row["issue-type"])
            ]
            if not mapping_row["description"].empty:
                row["description"] = mapping_row["description"].values[0]


class ColumnFieldLog(Log):
    fieldnames = ["dataset", "resource", "column", "field"]

    def add(self, column, field):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "column": column,
                "field": field,
            }
        )


class DatasetResourceLog(Log):
    fieldnames = [
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


class ConvertedResourceLog(Log):
    Success = "success"
    Failed = "failed"

    fieldnames = ["dataset", "resource", "status", "exception"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add(self, status, exception=""):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "status": status,
                "exception": exception,
            }
        )
