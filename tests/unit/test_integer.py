#!/usr/bin/env pytest

from digital_land.log import IssueLog
from digital_land.datatype.integer import IntegerDataType


def test_integer_format():
    integer = IntegerDataType()
    assert integer.format(1234567890) == "1234567890"


def test_integer_normalise():
    integer = IntegerDataType()
    assert integer.normalise("123") == "123"

    assert integer.normalise(" 0123 ") == "123"

    issues = IssueLog()
    assert integer.normalise("foo", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid integer"
    assert issue["value"] == "foo"
    assert issues.rows == []

    assert integer.normalise("12foo", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid integer"
    assert issue["value"] == "12foo"
    assert issues.rows == []

    integer = IntegerDataType(minimum=35, maximum=68)
    assert integer.normalise("38", issues=issues) == "38"
    assert issues.rows == []

    assert integer.normalise("34", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "too small"
    assert issue["value"] == "34"
    assert issues.rows == []

    assert integer.normalise("69", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "too large"
    assert issue["value"] == "69"
    assert issues.rows == []
