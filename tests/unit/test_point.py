from digital_land.issues import Issues
from digital_land.datatype.point import PointDataType


def test_point_normalise_wgs84():
    issues = Issues()
    point = PointDataType()

    # Nelson's column
    assert point.normalise(["51.507722", "-0.127972"]) == ["-0.127972", "51.507722"]
    assert point.normalise(["-0.127972", "51.507722"]) == ["-0.127972", "51.507722"]

    # Berwick-upon-Tweed
    assert point.normalise(["55.771", "-2.007"]) == ["-2.007", "55.771"]
    assert point.normalise(["-2.007", "55.771"]) == ["-2.007", "55.771"]

    # Scilly Isles
    assert point.normalise(["-6.322778", "49.936111"]) == ["-6.322778", "49.936111"]

    assert point.normalise(["-0.127972", "51.507722"], issues=issues) == [
        "-0.127972",
        "51.507722",
    ]
    assert issues.rows == []

    assert point.normalise(["-0.127972", ""], issues=issues) == ["", ""]
    assert issues.rows == []

    assert point.normalise(["", "51.507722"], issues=issues) == ["", ""]
    assert issues.rows == []

    assert point.normalise(["0.0", "0.0"], issues=issues) == ["", ""]
    issue = issues.rows.pop()
    assert issue["issue-type"] == "WGS84 outside England"
    assert issue["value"] == "0.0,0.0"
    assert issues.rows == []

    assert point.normalise(["0.0", "48.1"], issues=issues) == ["", ""]
    issue = issues.rows.pop()
    assert issue["issue-type"] == "WGS84 outside England"
    assert issue["value"] == "0.0,48.1"
    assert issues.rows == []


def test_point_normalise_northings_eastings():
    issues = Issues()
    point = PointDataType(precision=5)

    # Nelson's column TQ 30015 80415
    assert point.normalise(["530015", "180415"]) == ["-0.12799", "51.50772"]

    # Berwick-upon-Tweed  NT 99659 53073
    assert point.normalise(["399659", "653073"]) == ["-2.007", "55.77099"]

    # Scilly Isles SV 89926 12952
    assert point.normalise(["89926", "12952"]) == ["-6.32278", "49.93611"]

    # not possible to guess if they're swapped ..
    assert point.normalise(["12952", "89926"]) == ["", ""]
    assert point.normalise(["12952", "89926"], issues=issues) == ["", ""]
    issue = issues.rows.pop()
    assert issue["issue-type"] == "OSGB outside England"
    assert issue["value"] == "12952,89926"
    assert issues.rows == []


def test_point_normalise_out_of_range():
    issues = Issues()
    point = PointDataType(precision=5)

    assert point.normalise(["1000", "100000000"]) == ["", ""]
    assert point.normalise(["100000000", "10000"], issues=issues) == ["", ""]
    issue = issues.rows.pop()
    assert issue["issue-type"] == "out of range"
    assert issue["value"] == "100000000,10000"
    assert issues.rows == []


def test_point_normalise_osm():
    point = PointDataType(precision=5)
    assert point.normalise(["-7946.4687", "6701859.138"]) == ["-0.07138", "51.45323"]
