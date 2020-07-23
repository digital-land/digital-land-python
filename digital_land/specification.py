import csv
import os


class Specification:
    def __init__(self, path):
        self.dataset = {}
        self.dataset_names = []
        self.schema = {}
        self.schema_names = []

        self.load_dataset(path)
        self.load_schema(path)

    def load_dataset(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset.csv")))
        for row in reader:
            self.dataset_names.append(row["dataset"])
            self.dataset[row["dataset"]] = {"name": row["name"], "text": row["text"]}

    def load_schema(self, path):
        reader = csv.DictReader(open(os.path.join(path, "schema.csv")))
        for row in reader:
            self.schema_names.append(row["schema"])
            self.schema[row["schema"]] = {
                "name": row["name"],
                "description": row["description"],
            }
