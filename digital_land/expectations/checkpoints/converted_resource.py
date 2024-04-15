from pathlib import Path
from .base import BaseCheckpoint
from ..expectation_functions.resource_validations import (
    check_for_duplicate_references,
    validate_references,
)


class ConvertedResourceCheckpoint(BaseCheckpoint):
    def __init__(self, dataset_path, typology, dataset=None):
        super().__init__("converted_resource", dataset_path)
        self.csv_path = Path(dataset_path)
        if dataset:
            self.dataset = dataset
        else:
            self.dataset = self.csv_path.stem
        self.typology = typology

    def load(self):
        self.expectations = [
            {
                "function": check_for_duplicate_references(self.csv_path),
                "name": "Check for Duplicate References",
                "severity": "error",
                "responsibility": "system",
            },
            {
                "function": validate_references(self.csv_path),
                "name": "Validate References",
                "severity": "error",
                "responsibility": "system",
            },
        ]
