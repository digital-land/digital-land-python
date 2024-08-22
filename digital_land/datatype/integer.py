import re
from .datatype import DataType

strip_re = re.compile(r"\.0+$")


class IntegerDataType(DataType):
    # Setting default as 0 for minimum value. This will be applied for all integer data.
    def __init__(self, minimum=0, maximum=None):
        self.minimum = minimum
        self.maximum = maximum

    def format(self, value):
        return str(int(value))

    # Need to confirm messages
    def normalise(self, value, issues=None):
        value = strip_re.sub("", value, 1)
        try:
            n = int(value)
        except ValueError:
            if issues:
                issues.log(
                    "invalid integer",
                    value,
                    f"{issues.fieldname} must be a number",
                )
            return ""

        if self.minimum is not None and n < self.minimum:
            if issues:
                issues.log(
                    "too small",
                    value,
                    f"{issues.fieldname} must be larger than {self.minimum}",
                )
            return ""

        if self.maximum is not None and n > self.maximum:
            if issues:
                issues.log(
                    "too large",
                    value,
                    f"{issues.fieldname} must be lower than {self.maximum}",
                )
            return ""

        return self.format(n)
