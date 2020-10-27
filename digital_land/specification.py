from datetime import datetime
import csv
import os
import re

from .datatype.address import AddressDataType
from .datatype.date import DateDataType
from .datatype.decimal import DecimalDataType
from .datatype.integer import IntegerDataType
from .datatype.organisation import OrganisationURIDataType
from .datatype.string import StringDataType
from .datatype.uri import URIDataType
from .datatype.flag import FlagDataType
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

        self.load_dataset(path)
        self.load_schema(path)
        self.load_dataset_schema(path)
        self.load_datatype(path)
        self.load_field(path)
        self.load_schema_field(path)
        self.load_typology(path)

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

    @property
    def current_fieldnames(self):
        return [
            field
            for field, value in self.field.items()
            if not value["end-date"]
            or value["end-date"] > datetime.now().strftime("%Y-%m-%d")
        ]

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
        }

        if datatype in typemap:
            return typemap[datatype]()

        if fieldname in ["OrganisationURI"]:
            return OrganisationURIDataType()

        raise ValueError("unknown datatype '%s' for '%s' field" % (datatype, fieldname))
