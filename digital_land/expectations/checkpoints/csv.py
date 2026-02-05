import json
import duckdb
from pathlib import Path

from .base import BaseCheckpoint
from ..log import ExpectationLog
from ..operations.csv import (
    count_rows,
    check_unique,
    check_no_shared_values,
    check_no_overlapping_ranges,
)


class CsvCheckpoint(BaseCheckpoint):
    def __init__(self, dataset, file_path):
        self.dataset = dataset
        self.file_path = Path(file_path)
        self.log = ExpectationLog(dataset=dataset)

    def operation_factory(self, operation_string: str):
        operation_map = {
            "count_rows": count_rows,
            "check_unique": check_unique,
            "check_no_shared_values": check_no_shared_values,
            "check_no_overlapping_ranges": check_no_overlapping_ranges,
        }
        if operation_string not in operation_map:
            raise ValueError(
                f"Unknown operation: '{operation_string}'. Must be one of {list(operation_map.keys())}."
            )
        return operation_map[operation_string]

    def load(self, rules):
        self.expectations = []
        for rule in rules:
            expectation = {
                "operation": self.operation_factory(rule["operation"]),
                "name": rule["name"],
                "description": rule.get("description", ""),
                "dataset": self.dataset,
                "severity": rule.get("severity", ""),
                "responsibility": rule.get("responsibility", ""),
                "parameters": (
                    json.loads(rule["parameters"])
                    if isinstance(rule["parameters"], str)
                    else rule["parameters"]
                ),
            }
            self.expectations.append(expectation)

    def run_expectation(self, conn, expectation) -> tuple:
        params = expectation["parameters"]
        passed, msg, details = expectation["operation"](
            conn=conn, file_path=self.file_path, **params
        )
        return passed, msg, details

    def run(self):
        with duckdb.connect() as conn:
            for expectation in self.expectations:
                passed, message, details = self.run_expectation(conn, expectation)
                self.log.add(
                    {
                        "organisation": "",
                        "name": expectation["name"],
                        "passed": passed,
                        "message": message,
                        "details": details,
                        "description": expectation["description"],
                        "severity": expectation["severity"],
                        "responsibility": expectation["responsibility"],
                        "operation": expectation["operation"].__name__,
                        "parameters": expectation["parameters"],
                    }
                )

    def save(self, output_dir: Path):
        self.log.save_parquet(output_dir)
