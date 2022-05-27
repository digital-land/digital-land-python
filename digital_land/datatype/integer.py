import re
from .datatype import DataType

strip_re = re.compile(r"\.0+$")


class IntegerDataType(DataType):
    def __init__(self, minimum=None, maximum=None):
        self.minimum = minimum
        self.maximum = maximum

    def format(self, value):
        return str(int(value))

    def normalise(self, value, issues=None):
        value = strip_re.sub("", value, 1)
        try:
            n = int(value)
        except ValueError:
            if issues:
                issues.log("invalid integer", value)
            return ""

        if self.minimum is not None and n < self.minimum:
            if issues:
                issues.log("too small", value)
            return ""

        if self.maximum is not None and n > self.maximum:
            if issues:
                issues.log("too large", value)
            return ""

        return self.format(n)
