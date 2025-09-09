# digital_land/datatype/date.py

# --- Original imports (commented for review) ---
# from datetime import datetime
# from .datatype import DataType

# --- New imports (superset of original) ---
from datetime import datetime, date as _date
from typing import Optional, Union
from .datatype import DataType

FAR_FUTURE_YEARS_DEFAULT: int = 50  # default: today + 50 years


def _add_years(d: _date, years: int) -> _date:
    """Add 'years' (can be negative) to a date, handling Feb 29."""
    target_year = d.year + years
    try:
        return d.replace(year=target_year)
    except ValueError:
        # Handle Feb 29 -> Feb 28 on non-leap target years
        return d.replace(month=2, day=28, year=target_year)


def _coerce_to_date(v: Optional[Union[str, _date, datetime]]) -> Optional[_date]:
    """
    Accept date, datetime, or ISO 'YYYY-MM-DD' string and return a date.
    Return None if v is None. Raise TypeError for unsupported/invalid strings.
    """
    if v is None:
        return None
    if isinstance(v, _date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        try:
            return datetime.strptime(v.strip(), "%Y-%m-%d").date()
        except ValueError as e:
            raise TypeError(f"Invalid ISO date string for far_*_date: {v!r}") from e
    raise TypeError(
        f"far_*_date must be date/datetime/ISO string, not {type(v).__name__}"
    )


class DateDataType(DataType):
    # --- Legacy implementation (kept for reviewer context) ---
    # def normalise(self, fieldvalue, issues=None):
    #     value = fieldvalue.strip().strip('",')
    #
    #     # all of these patterns have been used!
    #     for pattern in [
    #         "%Y-%m-%d",
    #         "%Y%m%d",
    #         "%Y/%m/%d %H:%M:%S%z",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%d %H:%M:%S+00",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%d %H:%M:%S",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%d %H:%M",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S.000Z",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S.000",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S.%fZ",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S.%f%z",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%S.%f",  # added to handle ogr2ogr unix time conversion
    #         "%Y/%m/%dT%H:%M:%SZ",  # added to handle ogr2ogr unix time conversion
    #         "%Y-%m-%dT%H:%M:%S.000Z",
    #         "%Y-%m-%dT%H:%M:%S.000",
    #         "%Y-%m-%dT%H:%M:%S.%fZ",
    #         "%Y-%m-%dT%H:%M:%S.%f%z",
    #         "%Y-%m-%dT%H:%M:%S.%f",
    #         "%Y-%m-%dT%H:%M:%SZ",
    #         "%Y-%m-%dT%H:%M:%S",
    #         "%Y-%m-%d %H:%M:%S",
    #         "%Y/%m/%d",
    #         "%Y %m %d",
    #         "%Y.%m.%d",
    #         "%Y-%d-%m",  # risky!
    #         "%Y-%m",
    #         "%Y.%m",
    #         "%Y/%m",
    #         "%Y %m",
    #         "%Y",
    #         "%Y.0",
    #         "%d/%m/%Y %H:%M:%S",
    #         "%d/%m/%Y %H:%M",
    #         "%d-%m-%Y",
    #         "%d-%m-%y",
    #         "%d.%m.%Y",
    #         "%d.%m.%y",
    #         "%d/%m/%Y",
    #         "%d/%m/%y",
    #         "%d-%b-%Y",
    #         "%d-%b-%y",
    #         "%d %B %Y",
    #         "%b %d, %Y",
    #         "%b %d, %y",
    #         "%b-%y",
    #         "%B %Y",
    #         "%m/%d/%Y",  # risky!
    #         "%s",
    #     ]:
    #         try:
    #             date = datetime.strptime(value, pattern)
    #             return date.strftime("%Y-%m-%d")
    #         except ValueError:
    #             try:
    #                 if pattern == "%s":
    #                     date = datetime.utcfromtimestamp(float(value) / 1000.0)
    #                     return date.strftime("%Y-%m-%d")
    #                 if "%f" in pattern:
    #                     datearr = value.split(".")
    #                     if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
    #                         s = len(datearr[1].split("+")[0]) - 6
    #                         value = value.split("+")[0][:-s]
    #                     date = datetime.strptime(value, pattern)
    #                     return date.strftime("%Y-%m-%d")
    #             except ValueError:
    #                 pass
    #
    #     if issues:
    #         issues.log(
    #             "invalid date",
    #             fieldvalue,
    #             f"{issues.fieldname} must be a real date",
    #         )
    #     return ""

    # --- New implementation (adds date-based range checks; backward compatible) ---
    def normalise(
        self,
        fieldvalue,
        issues=None,
        *,
        far_future_date: Optional[Union[str, _date, datetime]] = None,
        far_past_date: Optional[Union[str, _date, datetime]] = None,
    ):
        """
        Normalise many date formats to YYYY-MM-DD.

        Far-future / far-past checks:
          - far_future_date: date/datetime/'YYYY-MM-DD' or None.
              If None, defaults to (today + 50 years).
              If provided, log 'far-future-date' when parsed_date > far_future_date.
          - far_past_date:   date/datetime/'YYYY-MM-DD' or None.
              Default None (ignored per AC). If provided, log 'far-past-date'
              when parsed_date < far_past_date.

        Notes:
          * Returns the normalised date string even when out of range; logs via `issues`.
          * Accepts None/non-string inputs safely.
        """
        # Prepare thresholds
        today = datetime.utcnow().date()
        eff_future = _coerce_to_date(far_future_date) if far_future_date is not None else _add_years(today, FAR_FUTURE_YEARS_DEFAULT)
        eff_past = _coerce_to_date(far_past_date) if far_past_date is not None else None

        # Robust value handling
        value = ("" if fieldvalue is None else str(fieldvalue)).strip().strip('",')
        if not value:
            if issues:
                issues.log(
                    "invalid date",
                    fieldvalue,
                    f"{getattr(issues, 'fieldname', 'date')} must be a real date",
                )
            return ""

        # All known/used patterns
        patterns = [
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y/%m/%d %H:%M:%S%z",  # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S+00",  # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S",  # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M",
            "%Y/%m/%dT%H:%M:%S",
            "%Y/%m/%dT%H:%M:%S.000Z",
            "%Y/%m/%dT%H:%M:%S.000",
            "%Y/%m/%dT%H:%M:%S.%fZ",
            "%Y/%m/%dT%H:%M:%S.%f%z",
            "%Y/%m/%dT%H:%M:%S.%f",
            "%Y/%m/%dT%H:%M:%SZ",
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
        ]

        for pattern in patterns:
            try:
                dt = datetime.strptime(value, pattern)
                d = dt.date()
                if eff_future is not None and d > eff_future and issues:
                    issues.log(
                        "far-future-date",
                        fieldvalue,
                        (
                            f"{getattr(issues, 'fieldname', 'date')} "
                            f"{d.isoformat()} is after far_future_date "
                            f"({eff_future.isoformat()})."
                        ),
                    )
                if eff_past is not None and d < eff_past and issues:
                    issues.log(
                        "far-past-date",
                        fieldvalue,
                        (
                            f"{getattr(issues, 'fieldname', 'date')} "
                            f"{d.isoformat()} is before far_past_date "
                            f"({eff_past.isoformat()})."
                        ),
                    )
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    if pattern == "%s":
                        # Treat as epoch milliseconds by default
                        dt = datetime.utcfromtimestamp(float(value) / 1000.0)
                        d = dt.date()
                        if eff_future is not None and d > eff_future and issues:
                            issues.log(
                                "far-future-date",
                                fieldvalue,
                                (
                                    f"{getattr(issues, 'fieldname', 'date')} "
                                    f"{d.isoformat()} is after far_future_date "
                                    f"({eff_future.isoformat()})."
                                ),
                            )
                        if eff_past is not None and d < eff_past and issues:
                            issues.log(
                                "far-past-date",
                                fieldvalue,
                                (
                                    f"{getattr(issues, 'fieldname', 'date')} "
                                    f"{d.isoformat()} is before far_past_date "
                                    f"({eff_past.isoformat()})."
                                ),
                            )
                        return dt.strftime("%Y-%m-%d")

                    if "%f" in pattern:
                        # Truncate over-precision fractional seconds to 6 digits
                        datearr = value.split(".")
                        if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                            s = len(datearr[1].split("+")[0]) - 6
                            value = value.split("+")[0][:-s]
                        dt = datetime.strptime(value, pattern)
                        d = dt.date()
                        if eff_future is not None and d > eff_future and issues:
                            issues.log(
                                "far-future-date",
                                fieldvalue,
                                (
                                    f"{getattr(issues, 'fieldname', 'date')} "
                                    f"{d.isoformat()} is after far_future_date "
                                    f"({eff_future.isoformat()})."
                                ),
                            )
                        if eff_past is not None and d < eff_past and issues:
                            issues.log(
                                "far-past-date",
                                fieldvalue,
                                (
                                    f"{getattr(issues, 'fieldname', 'date')} "
                                    f"{d.isoformat()} is before far_past_date "
                                    f"({eff_past.isoformat()})."
                                ),
                            )
                        return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        if issues:
            issues.log(
                "invalid date",
                fieldvalue,
                f"{getattr(issues, 'fieldname', 'date')} must be a real date",
            )
        return ""
