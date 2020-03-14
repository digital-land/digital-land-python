import re
from .datatype import DataType

comma_re = re.compile(r"(\s*,\s*){1,}")
hyphen_re = re.compile(r"(\s*-\s*){1,}")


class AddressDataType(DataType):
    def normalise(self, value, issues=None):

        # replace newlines and semi-colons with commas
        value = ", ".join(value.split("\n"))
        value = value.replace(";", ",")

        # normalise duplicate commas
        value = comma_re.sub(", ", value)
        value = value.strip(", ")

        # remove spaces around hyphens
        value = hyphen_re.sub("-", value)

        # remove double-quotes, normalise spaces
        value = " ".join(value.split()).replace('"', "").replace("”", "")

        return value
