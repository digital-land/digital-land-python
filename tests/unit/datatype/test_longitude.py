#!/usr/bin/env pytest

from digital_land.log import IssueLog
from digital_land.datatype.longitude import LongitudeDataType


def test_decimal_normalise():
    issues = IssueLog()
    longitude = LongitudeDataType()

    assert longitude.normalise("545764") == "545764"

    assert longitude.normalise("-1") == "-1"
    assert longitude.normalise("-1.123456789") == "-1.123457"

    assert longitude.normalise("1200.00") == "1200"
    assert longitude.normalise("1,200.00") == "1200"

    assert longitude.normalise("foo", issues=issues) == ""
    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid decimal"
    assert issue["value"] == "foo"
    assert issues.rows == []
