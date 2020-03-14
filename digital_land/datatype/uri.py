import re
import validators
from .datatype import DataType

end_of_uri_re = re.compile(r".*/")


def end_of_uri(value):
    return end_of_uri_re.sub("", value.rstrip("/").lower())


class URIDataType(DataType):
    def normalise(self, value):

        # fix URI values with line-breaks and spaces
        uri = "".join(value.split())

        if validators.url(uri):
            return uri

        if issues:
            issues.log("uri", value)

        return ""
