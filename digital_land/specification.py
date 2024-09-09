import csv
import os
from datetime import datetime
import logging
import warnings

from .datatype.address import AddressDataType
from .datatype.datatype import DataType
from .datatype.date import DateDataType
from .datatype.decimal import DecimalDataType
from .datatype.flag import FlagDataType
from .datatype.integer import IntegerDataType
from .datatype.organisation import OrganisationURIDataType
from .datatype.string import StringDataType
from .datatype.uri import URIDataType
from .datatype.point import PointDataType
from .datatype.multipolygon import MultiPolygonDataType

logger = logging.getLogger(__name__)

specification_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "specification"
)


class Specification:
    def __init__(self, path="specification"):
        self.dataset = {}
        self.dataset_names = []
        self.schema = {}
        self.schema_names = []
        self.dataset_schema = {}
        self.dataset_field = {}
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
        self.load_dataset_field(path)

        self.index_field()
        self.index_schema()

    def load_dataset(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset.csv")))
        for row in reader:
            self.dataset_names.append(row["dataset"])
            self.dataset[row["dataset"]] = row

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
            self.datatype[row["datatype"]] = row

    def load_field(self, path):
        reader = csv.DictReader(open(os.path.join(path, "field.csv")))
        for row in reader:
            self.field_names.append(row["field"])
            self.field[row["field"]] = row

    def load_dataset_field(self, path):
        reader = csv.DictReader(open(os.path.join(path, "dataset-field.csv")))
        for row in reader:
            fields = self.dataset_field.setdefault(row["dataset"], [])
            fields.append(row["field"])

    def load_schema_field(self, path):
        reader = csv.DictReader(open(os.path.join(path, "schema-field.csv")))
        for row in reader:
            self.schema_field.setdefault(row["schema"], [])
            self.schema_field[row["schema"]].append(row["field"])

    def load_typology(self, path):
        reader = csv.DictReader(open(os.path.join(path, "typology.csv")))
        for row in reader:
            self.typology[row["typology"]] = row

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

        return sorted(fields)

    def intermediate_fieldnames(self, pipeline):
        schema = self.pipeline[pipeline.name]["schema"]
        fieldnames = self.schema_field[schema].copy()
        replacement_fields = list(pipeline.migrations().keys())
        for field in replacement_fields:
            if field in fieldnames:
                fieldnames.remove(field)
        return sorted(fieldnames)

    def factor_fieldnames(self):
        return set(self.current_fieldnames("fact")).union(
            self.current_fieldnames("fact-resource")
        )

    def field_type(self, fieldname):
        warnings.warn(
            "depreciated use datatype_factory from the datatype module",
            DeprecationWarning,
            2,
        )

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
            "multipolygon": MultiPolygonDataType,
            "point": PointDataType,
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

    def field_dataset(self, fieldname):

        return self.field_dataset()

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

    def dataset_prefix(self, dataset):
        return self.dataset[dataset].get("prefix", "") or dataset

    def field_prefix(self, field):
        return self.field[field].get("prefix", "") or field

    def get_dataset_entity_min(self, dataset):
        if dataset:
            return self.dataset[dataset]["entity-minimum"]
        else:
            return 0

    def get_dataset_entity_max(self, dataset):
        if dataset:
            return self.dataset[dataset]["entity-maximum"]
        else:
            return 100

    def get_field_datatype_map(self):
        return {key: value["datatype"] for key, value in self.field.items()}

    def get_field_typology_map(self):
        return {key: self.field_typology(key) for key in self.field.keys()}

    def get_field_prefix_map(self):
        return {key: self.field_prefix(key) for key in self.field.keys()}

    def get_dataset_typology(self, dataset):
        if dataset:
            return self.dataset[dataset]["typology"]
        else:
            return None

    def get_category_fields(self, dataset):
        return [
            field
            for field in self.dataset_field.get(dataset, [])
            if dataset in self.dataset_field
            and field
            in [
                field
                for field, data in self.field.items()
                if data["typology"] == "category"
            ]
        ]
