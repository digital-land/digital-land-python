from .base import BaseCheckpoint
from ..core import QueryRunner
from ..expectation_functions.sqlite import check_old_entities


class DatasetCheckpoint(BaseCheckpoint):
    def __init__(self, checkpoint, dataset_path, dataset, typology):
        super().__init__(checkpoint, dataset_path)
        self.dataset_path = dataset_path
        self.dataset = dataset
        self.typolgy = typology

    def load(self):
        self.expectations = {
            check_old_entities: {
                "description": "Check for old entities",
                "severity": "warning",
                "query_runner": QueryRunner(self.dataset_path),
            },
        }
