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

        for row in csv.DictReader(open("specification/schema-field.csv", newline="")):
            self.schema[row["schema"]]["fields"].append(row["field"])


class Package:
    def __init__(self, datapackage, tables=[], indexes={}, specification=None):
        self.datapackage = datapackage
        self.tables = tables
        self.indexes = indexes
        if not specification:
            specification = Specification()
            specification.load()
        self.specification = specification
        self.join_tables = {}

    def create(self, path=None):
        pass
