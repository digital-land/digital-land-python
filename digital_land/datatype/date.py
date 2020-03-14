from datetime import datetime
from .datatype import DataType


class DateDataType(DataType):
    def normalise(self, fieldvalue, issues=None):
        value = fieldvalue.strip(' ",')

        # all of these patterns have been used!
        for pattern in [
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y-%m-%dT%H:%M:%S.000Z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y %m %d",
            "%Y.%m.%d",
            "%Y-%d-%m",  # risky!
            "%Y",
            "%Y.0",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%d.%m.%Y",
            "%d.%m.%y",
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%b-%Y",
            "%d-%b-%y",
            "%d %B %Y",
            "%b %d, %Y",
            "%b %d, %y",
            "%b-%y",
            "%m/%d/%Y",  # risky!
        ]:
            try:
                date = datetime.strptime(value, pattern)
                return date.strftime("%Y-%m-%d")
            except ValueError:
                pass

        if issues:
            issues.log("date", fieldvalue)

        return ""
