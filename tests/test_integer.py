from digital_land.issues import Issues
from digital_land.datatype.integer import IntegerDataType


def test_integer_format():
    integer = IntegerDataType()
    assert integer.format(1234567890) == "1234567890"


def test_integer_normalise():
    integer = IntegerDataType()
    assert integer.normalise("123") == "123"

    assert integer.normalise(" 0123 ") == "123"

    issues = Issues()
    assert integer.normalise("foo", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "integer"
    assert issue["value"] == "foo"
    assert issues.rows == []

    assert integer.normalise("12foo", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "integer"
    assert issue["value"] == "12foo"
    assert issues.rows == []
