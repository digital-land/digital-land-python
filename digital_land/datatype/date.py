from datetime import datetime
from datetime import date as _date
from .datatype import DataType
from calendar import monthrange


class DateDataType(DataType):

    def __init__(self, far_past_cutoff=_date(1799, 12, 31), future_years_ahead=50, today_provider=_date.today):
            """
            far_past_cutoff: dates strictly before this log 'far-past-date'
            future_years_ahead: how many years ahead from 'today' counts as far-future
            today_provider: callable returning today's date (inject in tests for determinism)
            """
            self.far_past_cutoff = far_past_cutoff
            self.future_years_ahead = future_years_ahead
            self.today_provider = today_provider

    def _future_cutoff(self):
        today = self.today_provider()
        y = today.year + self.future_years_ahead
        # keep same month/day if possible (handles Feb 29 & short months)
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)

    def normalise(self, fieldvalue, issues=None):
        value = fieldvalue.strip().strip('",')
        future_cutoff = self._future_cutoff()

        def _log_range(dt):
            if not issues:
                return
            d = dt.date()
            if d > future_cutoff:
                issues.log("far-future-date", fieldvalue,
                            f"{issues.fieldname} is more than {self.future_years_ahead} years in the future")
            if d < self.far_past_cutoff:
                issues.log("far-past-date", fieldvalue,
                            f"{issues.fieldname} is before {self.far_past_cutoff.isoformat()}")


        # all of these patterns have been used!
        for pattern in [
            "%Y-%m-%d",
            "%Y%m%d",
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
            "%s",
        ]:
            try:
                date = datetime.strptime(value, pattern)
                _log_range(date)  
                return date.date().isoformat()
            except ValueError:
                try:
                    if pattern == "%s":
                        date = datetime.utcfromtimestamp(float(value) / 1000.0)
                        _log_range(date)
                        return date.date().isoformat()
                    if "%f" in pattern:
                        datearr = value.split(".")
                        if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                            s = len(datearr[1].split("+")[0]) - 6
                            value = value.split("+")[0][:-s]
                        date = datetime.strptime(value, pattern)
                        _log_range(date)
                        return date.date().isoformat()

                except ValueError:
                    pass

        if issues:
            issues.log(
                "invalid date",
                fieldvalue,
                f"{issues.fieldname} must be a real date",
            )

        return ""
