import os

from .base import BaseCheckpoint
from ..utils import QueryRunner
from ..expectation_functions.sqlite import (
    check_old_entities,
    check_json_field_is_not_blank,
)

BASE = [
    {
        "function": check_old_entities,
        "name": "Check for retired entities in the entity table",
        "description": "Check for old entities",
        "severity": "warning",
        "responsbility": "internal",
    }
]

TYPOLOGY = {
    "document": [
        {
            "function": check_json_field_is_not_blank,
            "name": "Check entities in a document dataset have a document field",
            "severity": "warning",
            "responsibility": "external",
            "field": "document-url",
        }
    ],
}

DATASET = {
    "article-4-direction-area": [
        {
            "function": check_json_field_is_not_blank,
            "name": "Check article 4 direction area has an associated article 4 direction",
            "severity": "warning",
            "responsibility": "external",
            "field": "article-4-direction",
        }
    ]
}


class DatasetCheckpoint(BaseCheckpoint):
    def __init__(self, checkpoint, dataset_path, dataset, typology):
        super().__init__(checkpoint, dataset_path)
        self.dataset_path = dataset_path
        self.dataset = dataset
        self.typology = typology

    def load(self):
        self.expectations = []
        self.expectations.extend(BASE)
        typology_expectations = TYPOLOGY.get(self.typology, "")
        dataset_expectations = DATASET.get(self.dataset, "")

        if typology_expectations:
            self.expectations.extend(typology_expectations)

        if dataset_expectations:
            self.expectations.extend(dataset_expectations)

        for expectation in self.expectations:
            expectation["query_runner"] = QueryRunner(self.dataset_path)

    def save(self, output_dir, format="csv"):
        responses_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.data_name}-responses.csv"
        )
        issues_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.data_name}-issues.csv"
        )

        self.save_responses(
            self.responses,
            responses_file_path,
            format=format,
        )

        self.save_issues(self.issues, issues_file_path, format=format)
