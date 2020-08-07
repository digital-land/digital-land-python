from .datatype import DataType


class FlagDataType(DataType):
    def __init__(self):
        pass

    def normalise(self, value, issues=None):
        value = value.strip().lower()

        lookup = {
            "y": "yes",
            "n": "no",
        }

        value = lookup.get(value, value)

        if value in ["", "yes", "no"]:
            return value

        if issues:
            issues.log("flag", value)

        return ""
