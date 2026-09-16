import csv
import os
from datetime import datetime
import pandas as pd
import yaml
import pyarrow as pa
import pyarrow.parquet as pq


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
        self.entry_to_entity = {}

    def add(self, *args, **kwargs):
        pass

    def save(self, path=None, f=None):
        if not f:
            f = open(path, "w", newline="")
        writer = csv.DictWriter(f, self.fieldnames)
        writer.writeheader()
        for row in self.rows:
            writer.writerow(row)

    # Move this into save method when we decide to save all logs in .parquet?
    def save_parquet(self, output_dir):
        if output_dir:
            output_path = os.path.join(
                output_dir,
                "dataset=" + self.dataset,
                "resource=" + self.resource,
                self.resource + ".parquet",
            )
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            # Define the schema, using strings for non '-number' field
            schema = pa.schema(
                [
                    (field, pa.uint32() if "number" in field else pa.string())
                    for field in self.fieldnames
                ]
            )
            if len(self.rows) > 0:
                formatted_rows = self.rows.copy()
                for i, log in enumerate(formatted_rows):
                    formatted_rows[i] = {
                        key: log[key] if "number" in key else str(log[key])
                        for key in log
                    }
                table = pa.Table.from_pylist(formatted_rows, schema=schema)
            else:
                rows = [pa.array([], type=field.type) for field in schema]
                table = pa.Table.from_arrays(rows, schema=schema)

            pq.write_table(table, output_path)


class IssueLog(Log):
    fieldnames = [
        "dataset",
        "resource",
        "line-number",
        "entry-number",
        "field",
        "entity",
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
        self,
        fieldname,
        issue_type,
        value,
        message=None,
        line_number=0,
        entry_number=0,
        entity=None,
    ):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "field": fieldname,
                "issue-type": issue_type,
                "value": value,
                "entity": entity,
                "line-number": line_number or self.line_number,
                "entry-number": entry_number or self.entry_number,
                "message": message,
            }
        )

    def record_entity_map(self, entry, entity):
        if entry in self.entry_to_entity and self.entry_to_entity[entry] != entry:
            print(
                f"Entry {entry} is already mapped to {self.entry_to_entity[entry]}, trying to map to {entity}"
            )
        else:
            self.entry_to_entity[entry] = entity

    def apply_entity_map(self):
        for row in self.rows:
            if not row["entity"]:
                entity = self.entry_to_entity.get(row["entry-number"])
                if entity:
                    row["entity"] = entity

    def add_severity_column(self, severity_mapping_path=None, severity_mapping=None):
        # Load only the 'severity' column from severity_mapping, optional path if Specifiaction has already loaded it
        if severity_mapping is None and severity_mapping_path is None:
            raise ValueError(
                "Either severity_mapping_path or severity_mapping must be provided"
            )
        if severity_mapping is None:
            severity_mapping = pd.read_csv(
                severity_mapping_path,
                usecols=["issue-type", "severity", "description", "responsibility"],
            )
        elif isinstance(severity_mapping, dict):
            # Convert the dictionary to a DataFrame, keeping only relevant cols
            _cols = {"issue-type", "severity", "description", "responsibility"}
            severity_mapping = pd.DataFrame(
                [
                    {k: v for k, v in row.items() if k in _cols}
                    for row in severity_mapping.values()
                ]
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
            # Switch Pandas NaN to None to avoid issues with JSON
            self.rows = [
                {k: (None if pd.isna(v) else v) for k, v in row.items()}
                for row in merged_df.to_dict(orient="records")
            ]

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
        "entity-count",
        "entry-count",
        "line-count",
        "mime-type",
        "internal-path",
        "internal-mime-type",
        "code-version",
        "config-hash",
        "specification-hash",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entity_count = ""
        self.entry_count = 0
        self.line_count = 0
        self.mime_type = ""
        self.internal_path = ""
        self.internal_mime_type = ""
        self.code_version = ""
        self.config_hash = ""
        self.specification_hash = ""

    def add(self):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "entity-count": self.entity_count,
                "entry-count": self.entry_count,
                "line-count": self.line_count,
                "mime-type": self.mime_type,
                "internal-path": self.internal_path,
                "internal-mime-type": self.internal_mime_type,
                "code-version": self.code_version,
                "config-hash": self.config_hash,
                "specification-hash": self.specification_hash,
            }
        )

    def save(self, *args, **kwargs):
        self.add()
        super().save(*args, **kwargs)


class ConvertedResourceLog(Log):
    Success = "success"
    Failed = "failed"

    fieldnames = ["dataset", "resource", "elapsed", "status", "exception"]

    def add(self, elapsed, status, exception=""):
        self.rows.append(
            {
                "dataset": self.dataset,
                "resource": self.resource,
                "elapsed": elapsed,
                "status": status,
                "exception": exception,
            }
        )
