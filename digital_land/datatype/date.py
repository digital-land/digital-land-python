from datetime import datetime
from .datatype import DataType


class DateDataType(DataType):
    def normalise(self, fieldvalue, issues=None):
        value = fieldvalue.strip().strip('",')

        # all of these patterns have been used!
        for pattern in [
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y-%m-%dT%H:%M:%S.000Z",
            "%Y-%m-%dT%H:%M:%S.000",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%f",
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
            "%B %Y",
            "%m/%d/%Y",  # risky!
            "%s",
        ]:
            try:
                date = datetime.strptime(value, pattern)
                return date.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    if pattern == "%s":
                        date = datetime.utcfromtimestamp(float(value) / 1000.0)
                        return date.strftime("%Y-%m-%d")
                    if "%f" in pattern:
                        datearr = value.split(".")
                        if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                            s = len(datearr[1].split("+")[0]) - 6
                            value = value.split("+")[0][:-s]
                        date = datetime.strptime(value, pattern)
                        return date.strftime("%Y-%m-%d")

                except ValueError:
                    pass

        if issues:
            issues.log(
                "invalid date",
                fieldvalue,
                f"{issues.fieldname} must be a real date",
            )

        return ""
