import re
from .datatype import DataType

strip_re = re.compile(r"([^a-z0-9-_ ]+)")


field_enum = {}
field_value = {}


def normalise_value(value):
    return " ".join(strip_re.sub(" ", value.lower()).split())


def init():
    # load enum values from schema
    for field in schema["fields"]:
        fieldname = field["name"]
        if "constraints" in field and "enum" in field["constraints"]:
            field_enum.setdefault(fieldname, {})
            field_value.setdefault(fieldname, {})
            for enum in field["constraints"]["enum"]:
                value = normalise_value(enum)
                field_enum[fieldname][enum] = enum
                field_value[fieldname][value] = enum

    # load fix-ups from patch file
    for row in csv.DictReader(open("patch/enum.csv", newline="")):
        fieldname = row["field"]
        enum = row["enum"]
        value = normalise_value(row["value"])
        if enum not in field_enum[fieldname]:
            raise ValueError(
                "invalid '%s' enum '%s' in patch/enum.csv" % (fieldname, enum)
            )
        field_value.setdefault(fieldname, {})
        field_value[fieldname][value] = enum


class EnumDataType(DataType):
    def __init__(self, fieldname, values):
        pass

    def normalise(self, fieldvalue, issues=None):
        value = normalise_value(fieldvalue)

        if field in field_value and value in field_value[field]:
            return field_value[field][value]

        if issues:
            issue.log("enum", fieldvalue)

        return ""
