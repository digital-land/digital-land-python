from .datatype import DataType


class StringDataType(DataType):
    def __init__(self):
        pass

    def normalise(self, value, issues=None):
        value = value.strip()
        return value
