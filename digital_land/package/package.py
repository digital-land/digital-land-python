import csv


class Specification:
    def __init__(self):
        self.schema = {}
        self.field = {}

    def load(self):
        for row in csv.DictReader(open("specification/field.csv", newline="")):
            self.field[row["field"]] = row

        for row in csv.DictReader(open("specification/schema.csv", newline="")):
            self.schema[row["schema"]] = row
            self.schema[row["schema"]].setdefault("fields", [])
            self.schema[row["schema"]]["key-field"] = row["key-field"] or row["schema"]

        for row in csv.DictReader(open("specification/dataset.csv", newline="")):
            self.schema[row["dataset"]]["prefix"] = row["prefix"]
            self.schema[row["dataset"]]["typology"] = row["typology"]

        for row in csv.DictReader(open("specification/schema-field.csv", newline="")):
            self.schema[row["schema"]]["fields"].append(row["field"])


class Package:
    def __init__(
        self, datapackage, path=None, tables=[], indexes={}, specification=None
    ):
        self.datapackage = datapackage
        self.tables = tables
        self.indexes = indexes
        if not specification:
            specification = Specification()
            specification.load()
        self.specification = specification
        if not path:
            path = f"dataset/{self.datapackage}{self.suffix}"
        self.path = path

    def create(self, path=None):
        pass
