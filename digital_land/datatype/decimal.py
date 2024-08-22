from decimal import Decimal, InvalidOperation
from .datatype import DataType


class DecimalDataType(DataType):
    def __init__(self, precision=6, minimum=0.0, maximum=None):
        self.precision = precision
        self.minimum = minimum
        self.maximum = maximum

    def format(self, value):
        rounded_value = round(Decimal(value), self.precision).normalize()
        return str(self.remove_exponent(rounded_value))

    def normalise(self, value, issues=None):
        # remove commas ..
        value = value.replace(",", "")
        value = value.replace("£", "")

        try:
            d = Decimal(value)
        except InvalidOperation:
            if issues:
                issues.log(
                    "invalid decimal",
                    value,
                    f"{issues.fieldname} must be a decimal number",
                )
            return ""

        if self.minimum is not None and d < self.minimum:
            if issues:
                issues.log(
                    "too small",
                    value,
                    f"{issues.fieldname} must be larger than {self.minimum}",
                )
            return ""

        if self.maximum is not None and d > self.maximum:
            if issues:
                issues.log(
                    "too large",
                    value,
                    f"{issues.fieldname} must be lower than {self.maximum}",
                )
            return ""

        return self.format(d)

    # taken from FAQs in decimal documentation
    def remove_exponent(self, d):
        return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
