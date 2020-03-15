import re
from .datatype import DataType

strip_re = re.compile(r"([^a-z0-9-_ ]+)")


def normalise_value(value):
    return " ".join(strip_re.sub(" ", value.lower()).split())


class EnumDataType(DataType):

    enum = set()
    value = {}

    def __init__(self, enum=[]):
        self.add_enum(enum)

    def add_enum(self, enum):
        self.enum |= set(enum)
        for value in enum:
            self.add_value(value, value)

    def add_value(self, enum, value):
        if enum not in self.enum:
            raise ValueError
        self.value[normalise_value(value)] = enum

    def normalise(self, fieldvalue, issues=None):
        value = normalise_value(fieldvalue)

        if value in self.value:
            return self.value[value]

        if issues:
            issues.log("enum", fieldvalue)

        return ""
