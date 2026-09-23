import pytest
from datetime import date as _date

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
            ("20200102030459", "2020-01-02"),
            ("2024-07-02T13:49:47.676511", "2024-07-02"),
            ("2024-07-03T13:49:47.676511+01:00", "2024-07-03"),
            ("2024-07-04T13:41:46.7084023+01:00", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678+01:00", "2024-07-04"),
            ("2024-07-04T13:41:46.708402345678Z", "2024-07-04"),
            ("1609459200", "2021-01-01"),
            ("1609459200000", "2021-01-01"),
            ("1609459200000.0", "2021-01-01"),
            ("2009/03/30 00:00:00+00", "2009-03-30"),
            ("2013/04/15 00:00:00", "2013-04-15"),
            ("2013/04/15 00:00", "2013-04-15"),
            ("2024/07/02T13:49:47.676511", "2024-07-02"),
            ("2024/07/03T13:49:47.676511+01:00", "2024-07-03"),
            ("2024/07/04T13:41:46.7084023+01:00", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678+01:00", "2024-07-04"),
            ("2024/07/04T13:41:46.708402345678Z", "2024-07-04"),
            ("Thu, 11 Dec 2025 00:00:00 GMT", "2025-12-11"),
            ("1715123456789", "2024-05-07"),
            ("946684800", "2000-01-01"),
            ("1715123456", "2024-05-07"),
            ("171512345678", "1975-06-09"),
            ("-6048000000", "1969-10-23"),
            # 11-digit millisecond timestamps
            ("13392000000", "1970-06-05"),
            ("-49507200000", "1968-06-07"),
            ("10000000000", "1970-04-26"),
            ("99999999999", "1973-03-03"),
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

    @pytest.mark.parametrize(
        "input",
        ["2019-02-29", "foo", "123abc", "1609459200000foo", "16094592000000"],
    )
    def test_normalise_removes_invalid_values(self, input):
        issues = IssueLog()
        date = DateDataType()
        actual = date.normalise(input, issues)
        issue = issues.rows.pop()
        assert actual == ""
        assert issue["issue-type"] == "invalid date"
        assert issue["value"] == input
        assert issues.rows == []

    # ---------- test far future and far past date functionality ----------

    def test_normalise_far_future_date_exceeded(self):
        # Freeze "today" for determinism: 2025-01-15 -> future cutoff = 2075-01-15
        issues = IssueLog()
        issues.fieldname = "start-date"
        d = DateDataType(far_future_date=_date(2025, 1, 15))

        val = "2025-01-16"  # strictly greater than cutoff
        out = d.normalise(val, issues=issues)
        assert out == ""
        assert len(issues.rows) == 1
        issue = issues.rows.pop()
        assert issue["issue-type"] == "far-future-date"

    def test_normalise_far_future_date_not_exceeded(self):
        # Exactly on the cutoff should NOT log
        issues = IssueLog()
        d = DateDataType(far_future_date=_date(2025, 1, 15))

        val = "2025-01-15"  # exactly cutoff
        out = d.normalise(val, issues=issues)
        assert out == val
        assert issues.rows == []

    def test_normalise_far_past_date_exceeded(self):
        issues = IssueLog()
        issues.fieldname = "end-date"
        d = DateDataType(far_past_date=_date(1799, 12, 31))

        val = "1799-12-30"  # strictly before cutoff
        out = d.normalise(val, issues=issues)
        assert out == ""
        assert len(issues.rows) == 1
        issue = issues.rows.pop()
        assert issue["issue-type"] == "far-past-date"
        assert issue["value"] == val
        assert "before 1799-12-31" in issue["message"]
        assert issues.rows == []

    def test_normalise_far_past_date_not_exceeded(self):
        issues = IssueLog()
        d = DateDataType(far_past_date=_date(1799, 12, 31))

        val = "1799-12-31"  # boundary: not logged
        out = d.normalise(val, issues=issues)
        assert out == val
        assert issues.rows == []

    # --- ArcGIS endpoints: the encoding is known, so there is nothing to infer ---

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                "0",
                "1970-01-01",
            ),  # the reported case - a conservation area designated on the epoch
            ("-86400000", "1969-12-31"),  # pre-epoch, negative
            ("86400000", "1970-01-02"),
            ("-6048000000", "1969-10-23"),  # already worked; must keep the same answer
            ("1609459200000", "2021-01-01"),  # ordinary modern value, unchanged
        ],
    )
    def test_normalise_arcgis_values_are_always_milliseconds(self, input, expected):
        """ArcGIS REST encodes every date as a Unix millisecond timestamp, so where we know the
        endpoint used that plugin the digit-length heuristic is not needed.

        0 and the negatives are precisely the values the heuristic cannot reach: it requires 9 to
        13 digits and strips the sign before counting them.
        """
        issues = IssueLog()
        date = DateDataType(plugin="arcgis")

        assert date.normalise(input, issues) == expected
        assert issues.rows == []

    @pytest.mark.parametrize("input", ["0", "-86400000", "86400000"])
    def test_normalise_rejects_epoch_values_without_the_arcgis_plugin(self, input):
        """The bug being fixed, and the guard that the fix stays scoped to ArcGIS.

        Note normalise returns "" on failure, so today these values are discarded as well as
        flagged - the date does not reach the published record at all.
        """
        issues = IssueLog()

        assert DateDataType().normalise(input, issues) == ""
        assert len(issues.rows) == 1
        assert issues.rows[0]["issue-type"] == "invalid date"

    @pytest.mark.parametrize(
        "input,expected",
        [
            ("2020-01-02", "2020-01-02"),
            ("02/01/2020", "2020-01-02"),
            ("2020", "2020-01-01"),
            ("20200102", "2020-01-02"),
        ],
    )
    def test_normalise_arcgis_leaves_formatted_dates_alone(self, input, expected):
        """%s is the last pattern tried, so anything matching a real date format never reaches the
        ArcGIS branch. 2020 stays a year and 20200102 stays %Y%m%d rather than becoming 1970.
        """
        assert DateDataType(plugin="arcgis").normalise(input) == expected

    def test_normalise_arcgis_epoch_value_survives_the_far_past_check(self):
        """The harmoniser sets far_past_date to 1799-12-31, so a recovered 1970 date has to pass
        through rather than trade an invalid-date issue for a far-past-date one."""
        issues = IssueLog()
        d = DateDataType(far_past_date=_date(1799, 12, 31), plugin="arcgis")

        assert d.normalise("0", issues) == "1970-01-01"
        assert issues.rows == []

    def test_normalise_arcgis_reads_a_seconds_timestamp_as_milliseconds(self):
        """A deliberate trade-off, pinned here so it stays a decision rather than a surprise.

        We divide by 1000 unconditionally on the strength of the ArcGIS contract, so a value that
        was really in seconds becomes a 1970 date instead of the date it meant. This was checked
        against live data before being chosen: no 9- or 10-digit numeric values appear as invalid
        dates from any ArcGIS endpoint, and Esri documents milliseconds.
        """
        assert DateDataType(plugin="arcgis").normalise("1609459200") == "1970-01-19"
        assert DateDataType().normalise("1609459200") == "2021-01-01"
