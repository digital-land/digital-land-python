from .datatype import DataType


class StringDataType(DataType):
    def __init__(self):
        pass

    def normalise(self, value, issues=None):
        value = value.strip()

        # remove double-quotes, normalise spaces
        value = " ".join(value.split()).replace('"', "").replace("”", "")

        return value
