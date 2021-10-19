import csv
import os
import re
from datetime import datetime

from .datatype.address import AddressDataType
from .datatype.datatype import DataType
from .datatype.date import DateDataType
from .datatype.decimal import DecimalDataType
from .datatype.flag import FlagDataType
from .datatype.integer import IntegerDataType
from .datatype.organisation import OrganisationURIDataType
from .datatype.string import StringDataType
from .datatype.uri import URIDataType
from .datatype.wkt import WktDataType


class Specification:
    def __init__(self, path):
        self.dataset = {}
        self.dataset_names = []
        self.schema = {}
        self.schema_names = []
        self.dataset_schema = {}
        self.field = {}
        self.field_names = []
        self.datatype = {}
        self.datatype_names = []
        self.schema_field = {}
        self.typology = {}
        self.pipeline = {}

        self.load_dataset(path)
        self.load_schema(path)
        self.load_dataset_schema(path)
        self.load_datatype(path)
        self.load_field(path)
        self.load_schema_field(path)
        self.load_typology(path)
        self.load_pipeline(path)

        self.index_field()
        self.index_schema()

    def load_dataset(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset.csv")))
        for row in reader:
            self.dataset_names.append(row["dataset"])
            self.dataset[row["dataset"]] = {"name": row["name"], "text": row["text"]}

    def load_schema(self, path):
        reader = csv.DictReader(open(os.path.join(path, "schema.csv")))
        for row in reader:
            self.schema_names.append(row["schema"])
            self.schema[row["schema"]] = row

    def load_dataset_schema(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset-schema.csv")))
        for row in reader:
            schemas = self.dataset_schema.setdefault(row["dataset"], [])
            schemas.append(row["schema"])

    def load_datatype(self, path):
        reader = csv.DictReader(open(os.path.join(path, "datatype.csv")))
        for row in reader:
            self.datatype_names.append(row["datatype"])
            self.datatype[row["datatype"]] = {
                "name": row["name"],
                "text": row["text"],
            }

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
                "end-date": row["end-date"],
            }

    def load_schema_field(self, path):
        reader = csv.DictReader(open(os.path.join(path, "schema-field.csv")))
        for row in reader:
            self.schema_field.setdefault(row["schema"], [])
            self.schema_field[row["schema"]].append(row["field"])

    def load_typology(self, path):
        reader = csv.DictReader(open(os.path.join(path, "typology.csv")))
        for row in reader:
            self.typology[row["typology"]] = {
                "name": row["name"],
                "text": row["text"],
            }

    def load_pipeline(self, path):
        reader = csv.DictReader(open(os.path.join(path, "pipeline.csv")))
        for row in reader:
            self.pipeline[row["pipeline"]] = row

    def index_schema(self):
        self.schema_dataset = {}
        for dataset, d in self.dataset_schema.items():
            for schema in d:
                self.schema_dataset.setdefault(schema, [])
                self.schema_dataset[schema].append(dataset)

        self.schema_to = {}
        self.schema_from = {}
        for schema, s in self.schema_field.items():
            for name in s:
                field = self.base_field(name)
                if field != schema and field in self.schema:
                    self.schema_to.setdefault(schema, [])
                    if field not in self.schema_to[schema]:
                        self.schema_to[schema].append(field)

                    self.schema_from.setdefault(field, [])
                    if schema not in self.schema_from[field]:
                        self.schema_from[field].append(schema)

    def current_fieldnames(self, schema=None):
        if schema:
            fields = {}
            for f in self.schema_field[schema]:
                fields[f] = self.field[f]
        else:
            fields = self.field

        fields = [
            field
            for field, value in fields.items()
            if not value["end-date"]
            or value["end-date"] > datetime.now().strftime("%Y-%m-%d")
        ]

        # TBD: remove these hacks
        if "entity" not in fields:
            fields = ["entity"] + fields

        if "slug" not in fields:
            fields = fields + ["slug"]

        return fields

    normalise_re = re.compile(r"[^a-z0-9]")

    def normalise(self, name):
        return re.sub(self.normalise_re, "", name.lower())

    def field_type(self, fieldname):
        datatype = self.field[fieldname]["datatype"]
        typemap = {
            "integer": IntegerDataType,
            "decimal": DecimalDataType,
            "latitude": DecimalDataType,
            "longitude": DecimalDataType,
            "string": StringDataType,
            "address": AddressDataType,
            "text": StringDataType,  # TODO do we need dedicated type for Text?
            "datetime": DateDataType,
            "url": URIDataType,
            "flag": FlagDataType,
            "wkt": WktDataType,
            "curie": DataType,  # TODO create proper curie type
        }

        if datatype in typemap:
            return typemap[datatype]()

        if fieldname in ["OrganisationURI"]:
            return OrganisationURIDataType()

        raise ValueError("unknown datatype '%s' for '%s' field" % (datatype, fieldname))

    def field_parent(self, fieldname):
        field = self.field[fieldname]
        return field["parent-field"]

    def field_typology(self, fieldname):
        field = self.field[fieldname]
        if fieldname == field["parent-field"]:
            return fieldname
        return self.field_typology(field["parent-field"])

    def index_field(self):
        self.field_schema = {}
        for schema, s in self.schema_field.items():
            for field in s:
                self.field_schema.setdefault(field, [])
                self.field_schema[field].append(schema)

    def base_field(self, field):
        f = self.field[field]
        if f["cardinality"] == "1":
            return field
        return f["parent-field"]

    def key_field(self, schema):
        if schema not in self.schema:
            return ""
        if self.schema[schema].get("key-field", ""):
            return self.schema[schema]["key-field"]
        if schema in self.schema_field[schema]:
            return schema
        return ""
