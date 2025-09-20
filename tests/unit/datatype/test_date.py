import pytest

from digital_land.log import IssueLog
from digital_land.datatype.date import DateDataType


class TestDateDataType:
    @pytest.mark.parametrize(
        "input,expected",
        [
            # normal date formats
            ("2020-01-02", "2020-01-02"),
            ("20200102", "2020-01-02"),
            ("2020/01/02", "2020-01-02"),
            ("2020 01 02", "2020-01-02"),
            ("2020.01.02", "2020-01-02"),
            ("12 March 2020", "2020-03-12"),
            ("02-01-2020", "2020-01-02"),
            ("02.01.20", "2020-01-02"),
            ("02/01/2020", "2020-01-02"),
            ("02/01/20", "2020-01-02"),
            ("02-Jan-2020", "2020-01-02"),
            ("02-Jan-20", "2020-01-02"),
            ("2 January 2020", "2020-01-02"),
            ("Jan 2, 2020", "2020-01-02"),
            ("Jan 2, 20", "2020-01-02"),
            # date with less than 1000 years as the leading 0 can be removed
            ("0987-01-07", "0987-01-07"),
            # timestamp formats
            ("2020-01-02T03:04:59", "2020-01-02"),
            ("2020-01-02 03:04:59", "2020-01-02"),
            ("2020-01-02T03:04:59Z", "2020-01-02"),
            ("2024-07-02T13:49:47.676511", "2024-07-02"),
            ("2024-07-03T13:49:47.676511+01:00", "2024-07-03"),
            ("2024-07-04T13:41:46.7084023+01:00", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678+01:00", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678Z", "2024-07-04"),
            ("2009/03/30 00:00:00+00", "2009-03-30"),
            ("2013/04/15 00:00:00", "2013-04-15"),
            ("2013/04/15 00:00", "2013-04-15"),
            ("2024/07/02T13:49:47.676511", "2024-07-02"),
            ("2024/07/03T13:49:47.676511+01:00", "2024-07-03"),
            ("2024/07/04T13:41:46.7084023+01:00", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678+01:00", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678Z", "2024-07-04"),
            # years
            ("2020", "2020-01-01"),
            ("2020.0", "2020-01-01"),
            ("2020-01-02T03:04:59Z", "2020-01-02"),
            ("02/01/2020 03:04:59", "2020-01-02"),
            ("02/01/2020 03:04", "2020-01-02"),
            # months
            ("Jan-20", "2020-01-01"),
            ("1969-07", "1969-07-01"),
            ("1969.07", "1969-07-01"),
            ("1969/07", "1969-07-01"),
            ("1969 07", "1969-07-01"),
            #  risky attempts when it's clear american months are used
            ("2020-13-12", "2020-12-13"),
            ("13/12/2020", "2020-12-13"),
            # random found in wild the wild
            ("22/05/2018\xa0", "2018-05-22"),
        ],
    )
    def test_normalise_values_are_normalised_correctly_with__no_issues(
        self, input, expected
    ):
        date = DateDataType()
        assert date.normalise(input) == expected

        # with with issues
        issues = IssueLog()
        actual = date.normalise(input, issues)
        assert actual == expected
        assert len(issues.rows) == 0

    @pytest.mark.parametrize("input", ["2019-02-29", "foo"])
    def test_normalise_removes_invalid_values(self, input):
        issues = IssueLog()
        date = DateDataType()
        actual = date.normalise(input, issues)
        issue = issues.rows.pop()
        assert actual == ""
        assert issue["issue-type"] == "invalid date"
        assert issue["value"] == input
        assert issues.rows == []
