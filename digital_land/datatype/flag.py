from .datatype import DataType


class FlagDataType(DataType):
    def __init__(self):
        pass

    def normalise(self, value, issues=None):
        value = value.strip()
        if value not in ["", "yes", "no"]:
            raise ValueError()

        return value
