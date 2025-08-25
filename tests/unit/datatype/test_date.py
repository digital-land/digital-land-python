from datetime import datetime, timedelta

# import pytest

from digital_land.datatype.date import DateDataType
from digital_land.log import IssueLog


def _add_years(d, years):
    """Replicate the class's leap-safe year add for stable tests."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # handle Feb 29 -> Feb 28
        return d.replace(month=2, day=28, year=d.year + years)


def _issue_types(issues: IssueLog):
    return [row.get("issue-type") for row in getattr(issues, "rows", [])]


def test_normalise_basic_iso_and_epoch_ms_no_logs_when_field_not_checked():
    issues = IssueLog()
    issues.fieldname = "name"  # not in CHECK_FIELDS, so no range checks/logs

    d = DateDataType()

    # ISO date
    assert d.normalise("2021-01-02", issues=issues) == "2021-01-02"

    # Epoch milliseconds → 2021-01-01
    assert d.normalise("1609459200000", issues=issues) == "2021-01-01"

    assert len(issues.rows) == 0


def test_normalise_trims_long_microseconds():
    issues = IssueLog()
    issues.fieldname = "name"  # avoid range checks
    d = DateDataType()

    # 9 microsecond digits; implementation should trim to parse
    assert d.normalise("2021/01/02T03:04:05.123456789Z", issues=issues) == "2021-01-02"
    assert len(issues.rows) == 0


def test_far_future_logs_for_start_date_and_returns_value():
    today = datetime.utcnow().date()
    far_future = _add_years(today, 2).isoformat()

    issues = IssueLog()
    issues.fieldname = "start-date"  # in CHECK_FIELDS

    d = DateDataType(future_years_cutoff=1, past_years_cutoff=None)

    # Still returns the normalised date…
    assert d.normalise(far_future, issues=issues) == far_future
    # …but logs a far-future issue
    assert "far-future-date" in _issue_types(issues)


def test_far_past_logs_for_end_date_and_returns_value():
    today = datetime.utcnow().date()
    far_past = _add_years(today, -2).isoformat()

    issues = IssueLog()
    issues.fieldname = "end_date"  # underscore variant; should be normalised

    d = DateDataType(future_years_cutoff=None, past_years_cutoff=1)

    assert d.normalise(far_past, issues=issues) == far_past
    assert "far-past-date" in _issue_types(issues)


def test_fieldname_normalisation_and_per_call_override_future_cutoff():
    # Using per-call override: future_years_cutoff=0 means > today triggers
    tomorrow = (datetime.utcnow().date() + timedelta(days=1)).isoformat()

    issues = IssueLog()
    issues.fieldname = "Start Date"  # spaces/case variant; should match start-date

    d = DateDataType(future_years_cutoff=50)  # constructor default is loose
    result = d.normalise(tomorrow, issues=issues, future_years_cutoff=0)

    assert result == tomorrow
    assert "far-future-date" in _issue_types(issues)


def test_invalid_date_logs_and_returns_empty_string():
    issues = IssueLog()
    issues.fieldname = "start-date"

    d = DateDataType()
    assert d.normalise("not-a-date", issues=issues) == ""

    types = _issue_types(issues)
    assert "invalid date" in types
