#!/usr/bin/env pytest
import pytest

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
    issues.fieldname = "Hectares"
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
    assert issue["issue-type"] == "too small"
    assert issue["value"] == "-1"
    assert issue["message"] == f"{issues.fieldname} must be larger than 0.0"
    assert issues.rows == []


HECTARE_LIMITS = dict(
    minimum=Decimal("0.001"),
    soft_minimum=Decimal("0.01"),
    soft_maximum=Decimal("100"),
    maximum=Decimal("1000"),
)


@pytest.mark.parametrize(
    "value,expected,issue_type",
    [
        ("0", "", "too small"),
        ("0.0009", "", "too small"),
        ("0.001", "0.001", "smaller than expected"),
        ("0.005", "0.005", "smaller than expected"),
        ("0.01", "0.01", None),
        ("2.5", "2.5", None),
        ("100", "100", None),
        ("100.5", "100.5", "larger than expected"),
        ("931", "931", "larger than expected"),
        ("1000", "1000", "larger than expected"),
        ("1000.1", "", "too large"),
        ("400013", "", "too large"),
    ],
)
def test_decimal_normalise_hectare_limits(value, expected, issue_type):
    issues = IssueLog()
    issues.fieldname = "hectares"
    decimal = DecimalDataType(**HECTARE_LIMITS)

    assert decimal.normalise(value, issues=issues) == expected
    assert [issue["issue-type"] for issue in issues.rows] == (
        [issue_type] if issue_type else []
    )


def test_decimal_soft_limits_default_to_off():
    """Every existing caller omits the soft limits, so behaviour must be unchanged."""
    issues = IssueLog()
    issues.fieldname = "hectares"
    decimal = DecimalDataType()

    assert decimal.soft_minimum is None
    assert decimal.soft_maximum is None
    assert decimal.normalise("400013", issues=issues) == "400013"
    assert issues.rows == []
