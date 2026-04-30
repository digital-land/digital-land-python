import re
from datetime import datetime
from .datatype import DataType


class DateDataType(DataType):

    def __init__(self, far_past_date=None, far_future_date=None):
        """
        far_past_cutoff: dates strictly before this log 'far-past-date'
        future_years_ahead: how many years ahead from 'today' counts as far-future
        today_provider: callable returning today's date (inject in tests for determinism)
        """
        self.far_past_date = far_past_date
        self.far_future_date = far_future_date

    def normalise(self, fieldvalue, issues=None):
        value = fieldvalue.strip().strip('",')

        timestamp_pattern = re.compile(r"^[+-]?\d+(?:\.\d+)?$")

        # set date initially to None to be overriten if code is successful
        date = None
        # all of these patterns have been used!
        for pattern in [
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S%z",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S+00",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S.000Z",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S.000",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S.%fZ",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S.%f%z",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S.%f",  # added to handle ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%SZ",  # added to handle ogr2ogr unix time conversion
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
            "%Y-%m",
            "%Y.%m",
            "%Y/%m",
            "%Y %m",
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
            "%Y%m%d",
            "%Y%m%d%H%M%S",
            "%s",
        ]:
            try:
                if pattern == "%s":
                    if not timestamp_pattern.fullmatch(value):
                        continue

                    int_part = value.split(".", 1)[0].lstrip("+-")
                    if len(int_part) not in (10, 13):
                        continue

                    timestamp = float(value)
                    if len(int_part) == 13:
                        timestamp /= 1000.0
                    date = datetime.utcfromtimestamp(timestamp)
                    break

                if pattern == "%Y%m%d":
                    if not re.fullmatch(r"\d{8}", value):
                        continue

                if pattern == "%Y%m%d%H%M%S":
                    if not re.fullmatch(r"\d{14}", value):
                        continue

                if "%f" in pattern:
                    datearr = value.split(".")
                    if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                        s = len(datearr[1].split("+")[0]) - 6
                        value = value.split("+")[0][:-s]
                date = datetime.strptime(value, pattern)
                break
            except ValueError:
                pass

        if date is not None:
            if self.far_past_date and date.date() < self.far_past_date:
                if issues:
                    issues.log(
                        "far-past-date",
                        fieldvalue,
                        f"{value} is before {self.far_past_date.isoformat()}",
                    )
                return ""
            if self.far_future_date and date.date() > self.far_future_date:
                if issues:
                    issues.log(
                        "far-future-date",
                        fieldvalue,
                        f"{value} is after {self.far_future_date.isoformat()}",
                    )
                return ""

            return date.date().isoformat()

        if issues:
            issues.log(
                "invalid date",
                fieldvalue,
                f"{issues.fieldname} must be a real date",
            )

        return ""
