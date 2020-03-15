import re
import json

from .datatype.address import AddressDataType
from .datatype.decimal import DecimalDataType
from .datatype.date import DateDataType
from .datatype.enum import EnumDataType
from .datatype.integer import IntegerDataType
from .datatype.organisation import OrganisationURIDataType
from .datatype.string import StringDataType
from .datatype.uri import URIDataType


class Schema:

    """
    model for our extended frictionless data schema
    https://frictionlessdata.io/specs/table-schema/
    """

    normalise_re = re.compile(r"[^a-z0-9]")

    def __init__(self, path):
        self.schema = json.load(open(path))
        self.fields = {field["name"]: field for field in self.schema["fields"]}
        self.fieldnames = [field["name"] for field in self.schema["fields"]]

    def current_fieldnames(self):
        return [
            field["name"]
            for field in self.fields
            if not field["digital-land"].get("deprecated", False)
        ]

    def required_fieldnames(self):
        return [
            field["name"]
            for field in self.fields
            if field["constraints"].get("required", False)
        ]

    def default_fieldnames(self):
        return {
            field["name"]: field["digital-land"]["default"]
            for field in self.fields
            if field.get("digital-land", {}).get("default", None)
        }

    def normalise(self, name):
        return re.sub(self.normalise_re, "", name.lower())

    def typos(self):
        typos = {}
        for fieldname in self.fieldnames:
            field = self.fields[fieldname]
            typos[self.normalise(fieldname)] = fieldname
            if "title" in field:
                typos[self.normalise(field["title"])] = fieldname
            if "digital-land" in field:
                for typo in field["digital-land"].get("typos", []):
                    typos[self.normalise(typo)] = fieldname
        return typos

    def field_type(self, fieldname):
        field = self.fields[fieldname]
        constraints = field.get("constraints", {})
        extra = field.get("digital-land", {})

        if fieldname in ["OrganisationURI"]:
            return OrganisationURIDataType()

        if "enum" in field.get("constraints", {}):
            return EnumDataType(fieldname, enum=field["constraints"]["enum"])

        if field.get("type", "") == "date":
            return DateDataType()

        if field.get("type", "") == "integer":
            return IntegerDataType(
                minimum=constraints.get("minimum", None),
                maximum=constraints.get("maximum", None),
            )

        if field.get("type", "") == "number":
            return DecimalDataType(
                precision=extra.get("precision", None),
                minimum=constraints.get("minimum", None),
                maximum=constraints.get("maximum", None),
            )

        if field.get("format", "") == "address":
            return AddressDataType()

        if field.get("format", "") == "uri":
            return URIDataType()

        if field.get("type", "") in ["string", ""]:
            return StringDataType()

        raise ValueError("unknown datatype for '%s' field", fieldname)

    def strip(self, fieldname, value):
        field = self.schema.fields[fieldname]
        extra = field.get("digital-land", {})

        for strip in extra.get("strip", []):
            value = re.sub(strip, "", value)
        return value.strip()
