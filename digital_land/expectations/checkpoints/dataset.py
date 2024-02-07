from .base import BaseCheckpoint

import sqlite3

class DatasetCheckpoint(BaseCheckpoint):
    def __init__(self, dataset_path, dataset, typology):
        self.dataset_path = dataset_path
        self.dataset = dataset
        self.typolgy = typology

        self.connection = sqlite3.connect(self.dataset_path)

        print(f"DatasetCheckpoint path={dataset_path} dataset={dataset} typology={typology}")
