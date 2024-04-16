from pathlib import Path
from .base import BaseCheckpoint
from ..utils import QueryRunner
import os
from ..expectation_functions.resource_validations import (
    check_for_duplicate_references,
    validate_references,
)

# Define BASE expectations which should always run
BASE = [
    {
        "function": check_for_duplicate_references,
        "name": "Check for Duplicate References",
        "severity": "error",
        "responsibility": "system",
        "csv_path": None,
    },
    {
        "function": validate_references,
        "name": "Validate References",
        "severity": "error",
        "responsibility": "system",
        "csv_path": None,
    },
]

# Empty TYPOLOGY and DATASET for now as per advice
TYPOLOGY = {}
DATASET = {}


class ConvertedResourceCheckpoint(BaseCheckpoint):
    def __init__(self, dataset_path, typology, dataset=None):
        super().__init__("converted_resource", dataset_path)
        self.csv_path = Path(dataset_path)
        self.dataset = dataset if dataset else self.csv_path.stem
        self.typology = typology

    def load(self):
        self.expectations = []
        self.expectations.extend(BASE)
        typology_expectations = TYPOLOGY.get(self.typology, [])
        dataset_expectations = DATASET.get(self.dataset, [])

        # Extend the expectations list with relevant typology and dataset-specific expectations
        if typology_expectations:
            self.expectations.extend(typology_expectations)
        if dataset_expectations:
            self.expectations.extend(dataset_expectations)

        # Assign a QueryRunner instance to each expectation
        for expectation in self.expectations:
            expectation["csv_path"] = self.csv_path
            expectation["query_runner"] = QueryRunner(self.csv_path)

    def save(self, output_dir, format="csv"):
        responses_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.dataset}-responses.csv"
        )
        issues_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.dataset}-issues.csv"
        )

        self.save_responses(
            self.responses,
            responses_file_path,
            format=format,
        )

        self.save_issues(self.issues, issues_file_path, format=format)
