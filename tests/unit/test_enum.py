import pytest
from digital_land.log import IssueLog
from digital_land.datatype.enum import EnumDataType


def test_enum_normalise():
    issues = IssueLog()
    enum = EnumDataType(["apple", "orange", "banana"])

    assert enum.normalise("apple") == "apple"
    assert enum.normalise("Orange") == "orange"
    assert enum.normalise("BANANA") == "banana"
    assert enum.normalise("grape") == ""

    assert enum.normalise("grape", issues=issues) == ""
    issue = issues.rows.pop()
    assert issue["issue-type"] == "enum"
    assert issue["value"] == "grape"
    assert issues.rows == []

    with pytest.raises(ValueError):
        enum.add_value("grape", "raisin")

    enum.add_enum(["grape"])
    enum.add_value("grape", "raisin")

    assert enum.normalise("RAISIN", issues=issues) == "grape"
    assert issues.rows == []
