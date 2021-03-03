from decimal import Decimal, InvalidOperation
from .datatype import DataType


class DecimalDataType(DataType):
    def __init__(self, precision=6, minimum=None, maximum=None):
        self.precision = precision
        self.minimum = minimum
        self.maximum = maximum

    def format(self, value):
        rounded_value = round(Decimal(value), self.precision).normalize()
        return str(self.remove_exponent(rounded_value))

    def normalise(self, value, issues=None):
        # remove commas ..
        value = value.replace(",", "")

        try:
            d = Decimal(value)
        except InvalidOperation:
            if issues:
                issues.log("decimal", value)
            return ""

        if self.minimum is not None and d < self.minimum:
            if issues:
                issues.log("minimum", value)
            return ""

        if self.maximum is not None and d > self.maximum:
            if issues:
                issues.log("maximum", value)
            return ""

        return self.format(d)

    # taken from FAQs in decimal documentation
    def remove_exponent(self, d):
        return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
