from .datatype import DataType


class StringDataType(DataType):
    def format(self, value):
        return value

    def normalise(self, value, issues=None):
        return value
