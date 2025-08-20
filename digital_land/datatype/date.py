from datetime import datetime, date as _date
from typing import Optional, Tuple

from .datatype import DataType

# ============================
# User-configurable cutoffs
# ============================
FUTURE_YEARS_CUTOFF: Optional[int] = 50   # e.g. 50; set to None to ignore
PAST_YEARS_CUTOFF: Optional[int] = None   # e.g. 225; None = ignore past
CHECK_FIELDS: Tuple[str, ...] = ("start-date", "end-date")  # fields to check
# ============================


def _add_years(d: _date, years: int) -> _date:
    """Add 'years' (can be negative) to a date, handling Feb 29."""
    target_year = d.year + years
    try:
        return d.replace(year=target_year)
    except ValueError:
        # Handle Feb 29 -> Feb 28 on non-leap target years
        return d.replace(month=2, day=28, year=target_year)


class DateDataType(DataType):
    """
    Normalises many date formats to YYYY-MM-DD.

    Range checks (only for CHECK_FIELDS):
      - Far-future: flag if date > today + FUTURE_YEARS_CUTOFF (if not None)
      - Far-past:   flag if date < today - PAST_YEARS_CUTOFF   (if not None)

    Notes:
      * We still return the normalised date; we only log issues when out of range.
      * Field name comparison is normalised (case/space/_/- differences ignored).
    """

    @staticmethod
    def _norm_fieldname(s: str) -> str:
        # Treat "start-date", "start_date", and "Start Date" as the same key
        return s.strip().lower().replace(" ", "").replace("_", "").replace("-", "")

    def __init__(
        self,
        future_years_cutoff: Optional[int] = FUTURE_YEARS_CUTOFF,
        past_years_cutoff: Optional[int] = PAST_YEARS_CUTOFF,
        check_fields: Tuple[str, ...] = CHECK_FIELDS,
    ):
        super().__init__()
        self.future_years_cutoff: Optional[int] = future_years_cutoff
        self.past_years_cutoff: Optional[int] = past_years_cutoff
        self._check_fields = {self._norm_fieldname(f) for f in check_fields}

    def _should_check_field(self, issues) -> bool:
        if not issues or not getattr(issues, "fieldname", None):
            return False
        return self._norm_fieldname(issues.fieldname) in self._check_fields

    def _check_range_and_log(
        self,
        parsed_dt: datetime,
        raw_value: str,
        issues,
        future_years_cutoff: Optional[int],
        past_years_cutoff: Optional[int],
    ):
        if not self._should_check_field(issues):
            return

        d = parsed_dt.date()
        today = datetime.utcnow().date()

        # Far-future
        if future_years_cutoff is not None:
            future_cutoff = _add_years(today, future_years_cutoff)
            if d > future_cutoff:
                issues.log(
                    "far-future-date",
                    raw_value,
                    f"{issues.fieldname} {d.isoformat()} is more than "
                    f"{future_years_cutoff} years in the future (>{future_cutoff.isoformat()}).",
                )

        # Far-past
        if past_years_cutoff is not None:
            past_cutoff = _add_years(today, -past_years_cutoff)
            if d < past_cutoff:
                issues.log(
                    "far-past-date",
                    raw_value,
                    f"{issues.fieldname} {d.isoformat()} is more than "
                    f"{past_years_cutoff} years in the past (<{past_cutoff.isoformat()}).",
                )

    def normalise(
        self,
        fieldvalue,
        issues=None,
        future_years_cutoff: Optional[int] = None,
        past_years_cutoff: Optional[int] = None,
    ):
        """
        Optional per-call overrides:
          - future_years_cutoff: int or None
          - past_years_cutoff:   int or None
        If None, uses instance defaults from __init__.
        """
        eff_future = (
            self.future_years_cutoff if future_years_cutoff is None else future_years_cutoff
        )
        eff_past = (
            self.past_years_cutoff if past_years_cutoff is None else past_years_cutoff
        )

        value = fieldvalue.strip().strip('",')

        # all of these patterns have been used!
        for pattern in [
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y/%m/%d %H:%M:%S%z",  # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S+00",  # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M:%S",     # ogr2ogr unix time conversion
            "%Y/%m/%d %H:%M",        # ogr2ogr unix time conversion
            "%Y/%m/%dT%H:%M:%S",     # ogr2ogr unix time conversion
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
        ]:
            try:
                dt = datetime.strptime(value, pattern)
                self._check_range_and_log(dt, fieldvalue, issues, eff_future, eff_past)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    if pattern == "%s":
                        # Accept epoch milliseconds by default; seconds also work in some datasets
                        dt = datetime.utcfromtimestamp(float(value) / 1000.0)
                        self._check_range_and_log(dt, fieldvalue, issues, eff_future, eff_past)
                        return dt.strftime("%Y-%m-%d")
                    if "%f" in pattern:
                        datearr = value.split(".")
                        if len(datearr) > 1 and len(datearr[1].split("+")[0]) > 6:
                            s = len(datearr[1].split("+")[0]) - 6
                            value = value.split("+")[0][:-s]
                        dt = datetime.strptime(value, pattern)
                        self._check_range_and_log(dt, fieldvalue, issues, eff_future, eff_past)
                        return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        if issues:
            issues.log(
                "invalid date",
                fieldvalue,
                f"{issues.fieldname} must be a real date",
            )

        return ""
