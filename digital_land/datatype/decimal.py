from decimal import Decimal
from .datatype import DataType


class DecimalDataType(DataType):
    def __init__(self, precision=6, minimum=None, maximum=None):
        self.precision = precision
        self.minimum = minimum
        self.maximum = maximum

    def format(self, value):
        return str(round(Decimal(value), self.precision).normalize())

    def normalise(self, value, issues=None):
        try:
            d = Decimal(value)
        except Exception as e:
            if issues:
                issues.log("decimal", value)
            return ""

        if self.minimum != None and d < self.minimum:
            if issues:
                issues.log("minimum", value)
            return ""

        if self.maximum != None and d > self.maximum:
            if issues:
                issues.log("maximum", value)
            return ""

        return self.format(d)
