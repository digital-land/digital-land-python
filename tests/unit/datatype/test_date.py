#!/usr/bin/env pytest

import pytest
from digital_land.log import IssueLog
from digital_land.datatype.date import DateDataType


def test_basic_iso_normalise():
    d = DateDataType()
    assert d.normalise("2024-03-15") == "2024-03-15"


def test_strips_quotes_and_commas():
    d = DateDataType()
    assert d.normalise('  "2024-01-02",  ') == "2024-01-02"


def test_microseconds_trim():
    d = DateDataType()
    # 9 fractional digits + Z → should trim to 6 and parse
    assert d.normalise("2024-09-21T12:34:56.123456789Z") == "2024-09-21"


def test_epoch_milliseconds_parses():
    d = DateDataType()
    # 0 ms since epoch -> 1970-01-01
    assert d.normalise("0") == "1970-01-01"
    # 86_400_000 ms = 1 day -> 1970-01-02
    assert d.normalise("86400000") == "1970-01-02"


def test_far_future_date_logs_issue():
    issues = IssueLog()
    issues.fieldname = "Start date"
    d = DateDataType()

    out = d.normalise("3000-01-01", issues=issues)
    assert out == "3000-01-01"

    issue = issues.rows.pop()
    assert issue["issue-type"] == "far-future-date"
    assert issue["value"] == "3000-01-01"
    assert "more than 50 years in the future" in issue["message"]
    assert issues.rows == []


def test_far_past_date_logs_issue():
    issues = IssueLog()
    issues.fieldname = "End date"
    d = DateDataType()

    out = d.normalise("1500-01-01", issues=issues)
    assert out == "1500-01-01"

    issue = issues.rows.pop()
    assert issue["issue-type"] == "far-past-date"
    assert issue["value"] == "1500-01-01"
    assert "before 1799-12-31" in issue["message"]
    assert issues.rows == []


def test_invalid_date_logs_issue():
    issues = IssueLog()
    issues.fieldname = "Event date"
    d = DateDataType()

    assert d.normalise("not-a-date", issues=issues) == ""
    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid date"
    assert issue["value"] == "not-a-date"
    assert issue["message"] == f"{issues.fieldname} must be a real date"
    assert issues.rows == []


def test_year_and_year_month_defaults():
    d = DateDataType()
    assert d.normalise("2023") == "2023-01-01"
    assert d.normalise("2023-07") == "2023-07-01"