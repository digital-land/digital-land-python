from digital_land.log import IssueLog
from digital_land.datatype.date import DateDataType


def test_date_normalise():
    date = DateDataType()
    assert date.normalise("2020-01-02") == "2020-01-02"
    assert date.normalise("20200102") == "2020-01-02"
    assert date.normalise("2020-01-02T03:04:59Z") == "2020-01-02"
    assert date.normalise("2020-01-02T03:04:59") == "2020-01-02"
    assert date.normalise("2020-01-02 03:04:59") == "2020-01-02"
    assert date.normalise("2020/01/02") == "2020-01-02"
    assert date.normalise("2020 01 02") == "2020-01-02"
    assert date.normalise("2020.01.02") == "2020-01-02"
    assert date.normalise("12 March 2020") == "2020-03-12"
    assert date.normalise("2020") == "2020-01-01"
    assert date.normalise("2020.0") == "2020-01-01"
    assert date.normalise("02/01/2020 03:04:59") == "2020-01-02"
    assert date.normalise("02/01/2020 03:04") == "2020-01-02"
    assert date.normalise("02-01-2020") == "2020-01-02"
    assert date.normalise("02-01-20") == "2020-01-02"
    assert date.normalise("02.01.2020") == "2020-01-02"
    assert date.normalise("02.01.20") == "2020-01-02"
    assert date.normalise("02/01/2020") == "2020-01-02"
    assert date.normalise("02/01/20") == "2020-01-02"
    assert date.normalise("02-Jan-2020") == "2020-01-02"
    assert date.normalise("02-Jan-20") == "2020-01-02"
    assert date.normalise("2 January 2020") == "2020-01-02"
    assert date.normalise("Jan 2, 2020") == "2020-01-02"
    assert date.normalise("Jan 2, 20") == "2020-01-02"
    assert date.normalise("Jan-20") == "2020-01-01"

    # risky attempts ..
    assert date.normalise("2020-13-12") == "2020-12-13"
    assert date.normalise("13/12/2020") == "2020-12-13"

    # found this in the wild
    assert date.normalise("22/05/2018\xa0") == "2018-05-22"

    issues = IssueLog()
    assert date.normalise("2019-02-29", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "date"
    assert issue["value"] == "2019-02-29"
    assert issues.rows == []

    issues = IssueLog()
    assert date.normalise("foo", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "date"
    assert issue["value"] == "foo"
    assert issues.rows == []
