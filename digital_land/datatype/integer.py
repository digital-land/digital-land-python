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
        except Exception as e:
            if issues:
                issues.log("integer", value)
            return ""

        if self.minimum != None and n < self.minimum:
            if issues:
                issues.log("minimum %s" % (self.minumum), value)
            return ""

        if self.maximum != None and n > self.maximum:
            if issues:
                issues.log("maximum %s" % (self.maximum), value)
            return ""

        return self.format(n)
