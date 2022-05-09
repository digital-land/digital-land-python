#!/usr/bin/env -S py.test -svv

from digital_land.log import IssueLog
from digital_land.datatype.point import PointDataType


def issue_type(issues):
    if issues.rows == []:
        return None
    issue = issues.rows.pop()
    assert issues.rows == []
    return issue["issue-type"]


def test_point_wgs84():
    point = PointDataType()
    issues = IssueLog()

    # Nelson's colum
    assert point.normalise(["-0.127972", "51.507722"], issues=issues) == [
        "-0.127972",
        "51.507722",
    ]
    assert issue_type(issues) is None


def test_point_wgs84_south_west():
    point = PointDataType()
    issues = IssueLog()

    # Scilly Isles
    assert point.normalise(["-6.322778", "49.936111"], issues=issues) == [
        "-6.322778",
        "49.936111",
    ]
    assert issue_type(issues) is None


def test_point_wgs84_north_east():
    point = PointDataType()
    issues = IssueLog()

    # Berwick-upon-Tweed
    assert point.normalise(["-2.007", "55.771"], issues=issues) == ["-2.007", "55.771"]
    assert issue_type(issues) is None


def test_point_wgs84_flipped():
    point = PointDataType()
    issues = IssueLog()

    # Nelson's colum
    assert point.normalise(["51.507722", "-0.127972"], issues=issues) == [
        "-0.127972",
        "51.507722",
    ]
    assert issue_type(issues) == "WGS84 flipped"


def test_point_wgs84_out_of_range():
    point = PointDataType()
    issues = IssueLog()

    assert point.normalise(["0.0", "0.0"], issues=issues) == ["", ""]
    assert issue_type(issues) == "WGS84 out of bounds"

    assert point.normalise(["0.0", "48.1"], issues=issues) == ["", ""]
    assert issue_type(issues) == "WGS84 out of bounds"


def test_point_northings_eastings():
    issues = IssueLog()
    point = PointDataType()

    # Nelson's column TQ 30015 80415
    assert point.normalise(["530015", "180415"], issues=issues) == [
        "-0.12796",
        "51.507718",
    ]
    assert issue_type(issues) == "OSGB"


def test_point_flipped_northings_eastings():
    issues = IssueLog()
    point = PointDataType()

    # Nelson's column TQ 30015 80415
    assert point.normalise(["180415", "530015"], issues=issues) == [
        "-0.12796",
        "51.507718",
    ]
    assert issue_type(issues) == "OSGB flipped"


def test_point_mercator():
    issues = IssueLog()
    point = PointDataType()

    # Nelson's Column
    # https://epsg.io/map#srs=3857&x=-14245.780102&y=6711600.069496&z=17&layer=streets
    assert point.normalise(["-14245.780102", "6711600.069496"], issues=issues) == [
        "-0.127972",
        "51.507722",
    ]
    assert issue_type(issues) == "Mercator"


def test_point_mercator_flipped():
    issues = IssueLog()
    point = PointDataType()

    # Nelson's Column
    # https://epsg.io/map#srs=3857&x=-14245.780102&y=6711600.069496&z=17&layer=streets
    assert point.normalise(["6711600.069496", "-14245.780102"], issues=issues) == [
        "-0.127972",
        "51.507722",
    ]


def test_point_missing_values():
    point = PointDataType()
    issues = IssueLog()

    assert point.normalise(["", ""], issues=issues) == ["", ""]
    assert point.normalise(["-0.127972", ""], issues=issues) == ["", ""]
    assert point.normalise(["", "51.507722"], issues=issues) == ["", ""]


def test_point_out_of_range_values():
    point = PointDataType()
    issues = IssueLog()

    assert point.normalise(["1000", "100000000"], issues=issues) == ["", ""]
    assert issue_type(issues) == "invalid"

    assert point.normalise(["100000000", "10000"], issues=issues) == ["", ""]
    assert issue_type(issues) == "invalid"
