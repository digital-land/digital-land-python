import csv
import os
from datetime import datetime
import pandas as pd
import yaml
import logging
from .store.item import CSVItemStore
from .schema import Schema


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


class OperationalIssueLog(IssueLog):
    def __init__(self, dataset="", resource="", operational_issue_dir=None):
        super().__init__(dataset, resource)
        self.operational_issues = CSVItemStore(Schema("operational-issue"))
        self.operational_issue_dir = operational_issue_dir

    def get_now(self):
        return datetime.now().isoformat()

    def save(self, output_dir=None, path=None, f=None):
        if (
            not path and output_dir
        ):  # Create path if not specified and operational issue dir is given
            path = os.path.join(
                *[
                    output_dir,
                    self.dataset,
                    self.get_now()[:10],
                    self.resource + ".csv",
                ]
            )
        elif (
            not path
        ):  # Else if path not given and operational issue dir isn't specified then raise exception
            raise Exception(
                "Operational issue log directory/path or performance directory not given"
            )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        super().save(path=path, f=f)

    def load_log_items(self, operational_issue_directory=None, after=None):
        """
        Method to load the operational issue store from operational issue items instead of csvs. used when csvs don't exist
        or new issue items have been created by running the pipeline. If 'after' is not None, only log items after the
        specified date / time will be loaded.
        """
        operational_issue_directory = (
            operational_issue_directory or self.operational_issue_dir
        )

        logging.error("loading Operational issue files")
        self.operational_issues.load(
            directory=operational_issue_directory, after=after, dataset=self.dataset
        )

    def load(self, operational_issue_directory=None):
        operational_issue_directory = (
            operational_issue_directory or self.operational_issue_dir
        )
        # Try to load issue store from csv first
        try:
            self.operational_issues.load_csv(
                directory=os.path.join(operational_issue_directory, self.dataset)
            )
            logging.info("Operational Issues loaded from CSV")
        except FileNotFoundError:
            logging.error(
                "No operational_issue.csv - building from operational-issue items"
            )
            self.load_log_items(operational_issue_directory=operational_issue_directory)

    def update(self):
        self.load_log_items(after=self.operational_issues.latest_entry_date())

    def save_csv(self, directory=None):
        directory = directory or self.operational_issue_dir

        logging.error("saving csv")
        self.operational_issues.save_csv(
            directory=os.path.join(directory, self.dataset)
        )


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
