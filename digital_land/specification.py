import csv
import os


class Specification:
    def __init__(self, path):
        self.dataset = {}
        self.dataset_names = []
        self.schema = {}
        self.schema_names = []
        self.dataset_schema = {}
        self.field = {}
        self.field_names = []

        self.load_dataset(path)
        self.load_schema(path)
        self.load_dataset_schema(path)
        self.load_field(path)

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

    def load_dataset_schema(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset-schema.csv")))
        for row in reader:
            schemas = self.dataset_schema.setdefault(row["dataset"], [])
            schemas.append(row["schema"])

    def load_field(self, path):
        reader = csv.DictReader(open(os.path.join(path, "field.csv")))
        for row in reader:
            self.field_names.append(row["field"])
            self.field[row["field"]] = {
                "name": row["name"],
                "datatype": row["datatype"],
                "cardinality": row["cardinality"],
                "parent-field": row["parent-field"],
                "replacement-field": row["replacement-field"],
                "description": row["description"],
            }
