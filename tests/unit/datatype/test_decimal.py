#!/usr/bin/env pytest

from decimal import Decimal
from digital_land.log import IssueLog
from digital_land.datatype.decimal import DecimalDataType


pi = Decimal("3.1415926535897932384626433832795028841971")


def test_decimal_format():
    assert DecimalDataType().format(pi) == "3.141593"
    assert DecimalDataType(precision=12).format(pi) == "3.14159265359"
    assert DecimalDataType(precision=6).format(pi) == "3.141593"
    assert DecimalDataType(precision=7).format(pi) == "3.1415927"
    assert DecimalDataType(precision=2).format(pi) == "3.14"
    assert DecimalDataType(precision=1).format(pi) == "3.1"
    assert DecimalDataType(precision=0).format(pi) == "3"


def test_decimal_normalise():
    issues = IssueLog()
    decimal = DecimalDataType()

    assert decimal.normalise("00034.33520000") == "34.3352"

    assert decimal.normalise("foo") == ""

    assert decimal.normalise("foo", issues=issues) == ""

    assert decimal.normalise("1200.00") == "1200"
    assert decimal.normalise("1,200.00") == "1200"
    assert decimal.normalise("£1,200.00") == "1200"

    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid decimal"
    assert issue["value"] == "foo"
    assert issues.rows == []

    decimal = DecimalDataType(minimum=35, maximum=69)
    assert decimal.normalise("38.3", issues=issues) == "38.3"
    assert issues.rows == []

    assert decimal.normalise("34.3", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "too small"
    assert issue["value"] == "34.3"
    assert issues.rows == []

    assert decimal.normalise("69.9", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "too large"
    assert issue["value"] == "69.9"
    assert issues.rows == []

    decimal = DecimalDataType()

    decimal.normalise("-1", issues=issues)
    issue = issues.rows.pop()
    assert issue["issue-type"] == "numeric value is not positive"
    assert issue["value"] == "-1"
    assert issues.rows == []
