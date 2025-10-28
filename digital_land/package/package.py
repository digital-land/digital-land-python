import csv


class Specification:
    def __init__(
        self,
        specification_dir=None,
        schema=None,
        field=None,
    ):
        if schema is None:
            self.schema = {}
        else:
            self.schema = schema

        if field is None:
            self.field = {}
        else:
            self.field = field

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

    def get_field_datatype_map(self):
        """Get mapping of field names to their datatypes."""
        return {
            key: value.get("datatype", "string") for key, value in self.field.items()
        }

    def get_duckdb_type_mapping(self):
        """
        Get a mapping from specification datatypes to DuckDB SQL types.

        This mapping is used when creating parquet files or DuckDB tables
        to ensure correct type inference from specification.

        Returns:
            dict: Mapping of specification datatype to DuckDB SQL type
        """
        return {
            "integer": "BIGINT",
            "string": "VARCHAR",
            "text": "VARCHAR",
            "decimal": "DOUBLE",
            "date": "DATE",
            "datetime": "VARCHAR",
            "flag": "VARCHAR",
            "url": "VARCHAR",
            "uri": "VARCHAR",
            "curie": "VARCHAR",
            "point": "VARCHAR",
            "multipolygon": "VARCHAR",
            "address": "VARCHAR",
            "organisation": "VARCHAR",
            "latitude": "DOUBLE",
            "longitude": "DOUBLE",
        }

    def get_field_duckdb_type_map(self):
        """
        Get a mapping from field names to DuckDB SQL types.

        This combines get_field_datatype_map() with get_duckdb_type_mapping()
        to provide a direct field -> DuckDB type mapping.

        Returns:
            dict: Mapping of field name to DuckDB SQL type
        """
        field_datatype_map = self.get_field_datatype_map()
        duckdb_type_mapping = self.get_duckdb_type_mapping()

        return {
            field: duckdb_type_mapping.get(datatype, "VARCHAR")
            for field, datatype in field_datatype_map.items()
        }


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
