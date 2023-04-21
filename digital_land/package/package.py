import csv


class Specification:
    def __init__(self, specification_dir=None):
        self.schema = {}
        self.field = {}
        if specification_dir:
            self.specification_dir = specification_dir
        else:
            self.specification_dir = "specification"

    def load(self):
        for row in csv.DictReader(
            open(f"{self.specification_dir}/field.csv", newline="")
        ):
            self.field[row["field"]] = row

        for row in csv.DictReader(
            open(f"{self.specification_dir}/schema.csv", newline="")
        ):
            self.schema[row["schema"]] = row
            self.schema[row["schema"]].setdefault("fields", [])

        for row in csv.DictReader(
            open(f"{self.specification_dir}/dataset.csv", newline="")
        ):
            self.schema[row["dataset"]]["prefix"] = row["prefix"]
            self.schema[row["dataset"]]["typology"] = row["typology"]
            self.schema[row["dataset"]]["entity-minimum"] = row["entity-minimum"]
            self.schema[row["dataset"]]["entity-maximum"] = row["entity-maximum"]

        for row in csv.DictReader(
            open(f"{self.specification_dir}/schema-field.csv", newline="")
        ):
            self.schema[row["schema"]]["fields"].append(row["field"])


class Package:
    def __init__(
        self,
        datapackage,
        path=None,
        tables=[],
        indexes={},
        specification=None,
        specification_dir=None,
    ):
        self.datapackage = datapackage
        self.tables = tables
        self.indexes = indexes
        if not specification:
            specification = Specification(specification_dir)
            specification.load()
        self.specification = specification
        if not path:
            path = f"dataset/{self.datapackage}{self.suffix}"
        self.path = path

    def create(self, path=None):
        pass
