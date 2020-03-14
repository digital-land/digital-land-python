import validators
from .datatype import DataType


class URIDataType(DataType):
    def normalise(self, value, issues=None):

        # fix URI values with line-breaks and spaces
        uri = "".join(value.split())

        if validators.url(uri):
            return uri

        if issues:
            issues.log("uri", value)

        return ""
