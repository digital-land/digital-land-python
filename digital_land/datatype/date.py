from datetime import datetime, date as _date


class DataType:
    def format(self, value):
        return value

    def normalise(self, value, issues=None):
        return value


class DateDataType(DataType):
    def normalise(self, fieldvalue, issues=None):
        value = fieldvalue.strip().strip('",')

        
        # --- range boundaries (local to this method) ---
        FAR_PAST_CUTOFF = _date(1799, 12, 31)  # strictly before => far-past-date
        today = _date.today()
        try:
            future_cutoff = today.replace(year=today.year + 50)  # today + 50y
        except ValueError:  # handle Feb 29
            future_cutoff = today.replace(month=2, day=28, year=today.year + 50)

        def _log_range(dt):
            if not issues:
                return
            d = dt.date()
            if d > future_cutoff:
                issues.log("far-future-date", fieldvalue,
                           f"{issues.fieldname} is more than 50 years in the future")
            if d < FAR_PAST_CUTOFF:
                issues.log("far-past-date", fieldvalue,
                           f"{issues.fieldname} is before 1799-12-31")

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
                _log_range(date)                    # NEW
                return date.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    if pattern == "%s":
                        date = datetime.utcfromtimestamp(float(value) / 1000.0)
                        _log_range(date)            # <<< NEW
                        return date.strftime("%Y-%m-%d")
                    if "%f" in pattern:
                        datearr = value.split(".")
                        if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                            s = len(datearr[1].split("+")[0]) - 6
                            value = value.split("+")[0][:-s]
                        date = datetime.strptime(value, pattern)
                        _log_range(date)            # <<< NEW
                        return date.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        if issues:
            issues.log(
                "invalid date", 
                fieldvalue, 
                f"{issues.fieldname} must be a real date"
                )
            
        return ""
