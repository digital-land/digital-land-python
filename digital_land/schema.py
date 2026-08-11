from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from typing import ClassVar

from pydantic import BaseModel

# TBD: make part of the specification module and use data from:
# https://digital-land.github.io/specification/
schemas = {
    "log": {
        "fields": [
            "bytes",
            "content-type",
            "elapsed",
            "endpoint",
            "resource",
            "status",
            "entry-date",
            "start-date",
            "end-date",
            "exception",
        ],
        "key": "endpoint",
    },
    "endpoint": {
        "fields": [
            "endpoint",
            "endpoint-url",
            "parameters",
            "plugin",
            "entry-date",
            "start-date",
            "end-date",
        ],
        "key": "endpoint",
    },
    "source": {
        "fields": [
            "source",
            "attribution",
            "collection",
            "documentation-url",
            "endpoint",
            "licence",
            "organisation",
            "pipelines",
            "entry-date",
            "start-date",
            "end-date",
        ],
        "key": "endpoint",
    },
    "resource": {
        "fields": [
            "resource",
            "bytes",
            "organisations",
            "datasets",
            "endpoints",
            "start-date",
            "end-date",
        ],
        "key": "resource",
    },
    "old-resource": {
        "fields": [
            "old-resource",
            "resource",
            "status",
        ],
        "key": "old-resource",
    },
    "lookup": {
        "key": "lookup",
        "fields": [
            "prefix",
            "resource",
            "endpoint",
            "entry-number",
            "organisation",
            "reference",
            "entity",
            "entry-date",
            "start-date",
            "end-date",
        ],
    },
    "lookup-rule": {
        "key": "lookup-rule",
        "fields": [
            "prefix",
            "dataset",
            "organisation",
            "resource",
            "offset",
            "entity-minimum",
            "entity-maximum",
            "entry-date",
            "start-date",
            "end-date",
        ],
    },
    "operational-issue": {
        "fields": [
            "dataset",
            "resource",
            "line-number",
            "entry-number",
            "field",
            "issue-type",
            "value",
            "message",
            "entry-date",
        ],
        "key": "operational-issue",
    },
}


class Field:
    "information about a field"

    def __init__(self, name):
        self.name = name


class Schema:
    # TBD: make a singleton for each name
    def __init__(self, name):
        self.name = name
        self.key = schemas[name]["key"]
        self.field = OrderedDict()
        for field in schemas[name]["fields"]:
            self.field[field] = Field(field)
        self.fieldnames = self.field.keys()


class FieldSchema(BaseModel):
    "the specification for one field, as used by a dataset"

    field: str
    datatype: str
    cardinality: str
    parent_field: str = ""
    typology: str = ""

    # attributes that feed hash_payload()/hash(). Adding a new attribute to
    # this model does NOT change the hash until it's added here too.
    HASH_FIELDS: ClassVar[tuple[str, ...]] = (
        "field",
        "datatype",
        "cardinality",
        "parent_field",
        "typology",
    )

    def hash_payload(self) -> dict:
        return self.model_dump(include=set(self.HASH_FIELDS))

    def hash(self) -> str:
        payload = json.dumps(self.hash_payload(), sort_keys=True, default=str)
        return hashlib.sha1(payload.encode()).hexdigest()


class DatasetSchema(BaseModel):
    "everything needed to identify and process one dataset"

    dataset: str
    prefix: str
    typology: str
    entity_minimum: int
    entity_maximum: int
    key_field: str
    fields: dict[str, FieldSchema] = {}

    # attributes that feed hash_payload()/hash(). "dataset" (the name) is
    # deliberately excluded: it's an identifier, not something that changes
    # how the dataset is processed. Adding a new attribute to this model
    # does NOT change the hash until it's added here too.
    HASH_FIELDS: ClassVar[tuple[str, ...]] = (
        "prefix",
        "typology",
        "entity_minimum",
        "entity_maximum",
        "key_field",
    )

    def hash_payload(self) -> dict:
        payload = self.model_dump(include=set(self.HASH_FIELDS))
        payload["fields"] = {
            name: field.hash_payload() for name, field in self.fields.items()
        }
        return payload

    def hash(self) -> str:
        payload = json.dumps(self.hash_payload(), sort_keys=True, default=str)
        return hashlib.sha1(payload.encode()).hexdigest()
