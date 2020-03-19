import re
import csv
from .datatype import DataType

strip_re = re.compile(r"([^a-z0-9-_ ]+)")


class EnumDataType(DataType):
    def __init__(self, enum=[], name="enum", patches_path=None, dataset=None):
        self.name = name
        self.enum = set()
        self.value = {}
        self.add_enum(enum)

        if dataset and self.load_dataset:
            self.load_dataset(dataset)

        if patches_path:
            self.load_patches(patches_path)

    def add_enum(self, enum):
        if type(enum) not in (list, tuple):
            enum = [enum]
        self.enum |= set(enum)
        for value in enum:
            self.add_value(value, value)

    def add_value(self, enum, value):
        if enum not in self.enum:
            raise ValueError
        self.value[self.normalise_value(value)] = enum

    def load_patches(self, path):
        for row in csv.DictReader(open(path, newline="")):
            self.add_value(row["enum"], row["value"])

    def normalise_value(self, value):
        return " ".join(strip_re.sub(" ", value.lower()).split())

    def normalise(self, fieldvalue, issues=None):
        value = self.normalise_value(fieldvalue)

        if value in self.value:
            return self.value[value]

        if issues:
            issues.log(self.name, fieldvalue)

        return ""
