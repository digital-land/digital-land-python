import csv
import json
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path


# TODO Review and connect to centralised Log classes do we need our own class for this
class ExpectationLog:
    """
    a class to create and store the log output from running expectations

    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.entries = []
        self.fieldnames = [
            "dataset",
            "organisation",
            "name",
            "passed",
            "parameters",
            "message",
            "details",
            "description",
            "severity",
            "responsibility",
        ]
        # create the parquet partition for saving output each log is assumed to
        # write to a particular partition of the data
        self.pq_partition = f"dataset={dataset}"
        # create a pyarrow schema from the fields This helps ensure
        # parquet data is saved correctlly and blank partitions can be saved
        self.pq_schema = pa.schema(
            [
                (field, pa.uint32() if "number" in field else pa.string())
                for field in self.fieldnames
            ]
        )

    def add(self, entry: dict):
        """
        function to add an individual log respresented as a dictionary
        """
        # for json fields we want to add it as a json string
        params = entry["parameters"]
        if isinstance(params, dict):
            params = json.dumps(params)

        details = entry["details"]
        if isinstance(details, dict):
            details = json.dumps(details)
        self.entries.append(
            {
                "dataset": self.dataset,
                "organisation": entry.get("organisation", ""),
                "name": entry["name"],
                "passed": entry["passed"],
                "message": entry["message"],
                "details": details,
                "description": entry.get("description", ""),
                "severity": entry.get("severity", ""),
                "responsibility": entry.get("responsibility", ""),
                "operation": entry["operation"],
                "parameters": params,
            }
        )

    def save(self, path=None, f=None):
        if not f:
            f = open(path, "w", newline="")
        writer = csv.DictWriter(f, self.fieldnames)
        writer.writeheader()
        for row in self.rows:
            writer.writerow(row)

    def save_parquet(self, output_dir, partition=None):
        """
        Save the output into a parquet file that is a partition
        for this particular dataset. Unlike saving to a csv this uses a directory
        to add the partition.
        """
        partition = partition or self.pq_partition

        output_dir = Path(output_dir)
        output_path = output_dir / partition / (self.dataset + ".parquet")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # separate process for writing to partition if there are entries
        if len(self.entries) > 0:
            formatted_entries = self.entries.copy()
            # TODO can entries be fomatted on input could be time to
            # dataclasses
            for i, log in enumerate(formatted_entries):
                formatted_entries[i] = {
                    key: log[key] if "number" in key else str(log[key]) for key in log
                }
            table = pa.Table.from_pylist(formatted_entries, schema=self.pq_schema)
        else:
            rows = [pa.array([], type=field.type) for field in self.pq_schema]
            table = pa.Table.from_arrays(rows, schema=self.pq_schema)

        pq.write_table(table, output_path)
