#!/usr/bin/env pytest

from digital_land.log import IssueLog
from digital_land.datatype.latitude import LatitudeDataType


def test_decimal_normalise():
    issues = IssueLog()
    latitude = LatitudeDataType()

    assert latitude.normalise("257267") == "257267"

    assert latitude.normalise("-1") == "-1"
    assert latitude.normalise("-3.77687") == "-3.77687"

    assert latitude.normalise("1200.00") == "1200"
    assert latitude.normalise("1,200.00") == "1200"

    assert latitude.normalise("foo", issues=issues) == ""
    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid decimal"
    assert issue["value"] == "foo"
    assert issues.rows == []
