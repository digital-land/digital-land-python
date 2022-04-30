#!/usr/bin/env pytest

from digital_land.datatype.wkt import WktDataType
from digital_land.log import IssueLog


def issue_type(issues):
    if issues.rows == []:
        return None
    issue = issues.rows.pop()
    assert issues.rows == []
    return issue["issue-type"]


def test_wkt_point_wgs84():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's colum
    assert (
        wkt.normalise("POINT( -0.127972   51.507722 )", issues=issues)
        == "POINT (-0.127972 51.507722)"
    )
    assert issue_type(issues) is None


def test_wkt_point_wgs84_south_west():
    wkt = WktDataType()
    issues = IssueLog()

    # Scilly Isles
    assert (
        wkt.normalise("POINT (-6.322778 49.936111)", issues=issues)
        == "POINT (-6.322778 49.936111)"
    )
    assert issue_type(issues) is None


def test_wkt_point_wgs84_north_east():
    wkt = WktDataType()
    issues = IssueLog()

    # Berwick-upon-Tweed
    assert (
        wkt.normalise("POINT (-2.007 55.771)", issues=issues)
        == "POINT (-2.007000 55.771000)"
    )
    assert issue_type(issues) is None


def test_wkt_point_wgs84_flipped():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's colum
    assert (
        wkt.normalise("POINT (51.507722 -0.127972)", issues=issues)
        == "POINT (-0.127972 51.507722)"
    )
    assert issue_type(issues) == "WGS84 flipped"


def test_wkt_point_wgs84_out_of_range():
    wkt = WktDataType()
    issues = IssueLog()

    assert wkt.normalise("POINT (0.0 0.0)", issues=issues) == ""
    assert issue_type(issues) == "WGS84 out of bounds"

    assert wkt.normalise("POINT (0.0 48.1)", issues=issues) == ""
    assert issue_type(issues) == "WGS84 out of bounds"


def test_wkt_point_northings_eastings():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's column TQ 30015 80415
    assert (
        wkt.normalise("POINT (530015 180415)", issues=issues)
        == "POINT (-0.127985 51.507717)"
    )
    assert issue_type(issues) == "OSGB"


def test_wkt_point_flipped_northings_eastings():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's column TQ 30015 80415
    assert (
        wkt.normalise("POINT (180415 530015)", issues=issues)
        == "POINT (-0.127985 51.507717)"
    )
    assert issue_type(issues) == "OSGB flipped"


def test_wkt_point_mercator():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's Column
    # https://epsg.io/map#srs=3857&x=-14245.780102&y=6711600.069496&z=17&layer=streets
    assert (
        wkt.normalise("POINT (-14245.780102 6711600.069496)", issues=issues)
        == "POINT (-0.127972 51.507722)"
    )
    assert issue_type(issues) == "Mercator"


def test_wkt_point_mercator_flipped():
    wkt = WktDataType()
    issues = IssueLog()

    # Nelson's Column
    # https://epsg.io/map#srs=3857&x=-14245.780102&y=6711600.069496&z=17&layer=streets
    assert (
        wkt.normalise("POINT (6711600.069496 -14245.780102)", issues=issues)
        == "POINT (-0.127972 51.507722)"
    )


def test_wkt_point_missing_values():
    wkt = WktDataType()
    issues = IssueLog()

    assert wkt.normalise("POINT", issues=issues) == ""
    assert issue_type(issues) == "invalid WKT"

    assert wkt.normalise("POINT ()", issues=issues) == ""
    assert issue_type(issues) == "invalid WKT"

    assert wkt.normalise("POINT (-0.127972 )", issues=issues) == ""
    assert issue_type(issues) == "invalid WKT"


def test_wkt_point_out_of_range_values():
    wkt = WktDataType()
    issues = IssueLog()

    assert wkt.normalise("POINT (1000 100000000)", issues=issues) == ""
    assert issue_type(issues) == "invalid"

    assert wkt.normalise("POINT (100000000 10000)", issues=issues) == ""
    assert issue_type(issues) == "invalid"


def test_wkt_multipolygon_wgs84():
    wkt = WktDataType()
    issues = IssueLog()

    value = "MULTIPOLYGON (((-0.1434494279 51.46626361,-0.1434646353 51.46627914,-0.143515539 51.4663375,-0.1435648475 51.4663926,-0.1435988703 51.46643054,-0.1436227923 51.46646195,-0.1436840978 51.46644134,-0.1436913831 51.4664392,-0.1437519691 51.46641858,-0.1437548832 51.46641773,-0.1436953554 51.46634835,-0.1435837312 51.46621808,-0.1435209507 51.46623957,-0.1434494279 51.46626361)))"  # noqa: E501
    expected = "MULTIPOLYGON (((-0.143449 51.466264,-0.143623 51.466462,-0.143755 51.466418,-0.143584 51.466218,-0.143449 51.466264)))"
    assert wkt.normalise(value, issues=issues) == expected
    assert issue_type(issues) is None


def test_wkt_multipolygon_flipped_northings_and_eastings():
    wkt = WktDataType()
    issues = IssueLog()
    value = "MULTIPOLYGON (((203500.0 494297.28,203499.8 494297.07,203495.1 494292.05,203491.2 494287.55,203487.2 494284.05,203482.45 494280.05,203478.4 494276.3,203479.85 494274.9,203486.95 494265.96,203500.0 494249.55,203503.6 494244.7,203514.5 494230.45,203532.6 494206.8,203554.0 494178.8,203566.2 494162.9,203601.8 494116.4,203626.0 494136.2,203628.4 494138.7,203628.9 494141.1,203626.04 494151.35,203639.0 494160.6,203645.3 494165.9,203650.7 494170.2,203651.8 494170.9,203659.0 494175.7,203670.3 494183.0,203674.8 494186.0,203677.7 494188.6,203619.4 494261.3,203612.3 494270.3,203607.2 494276.8,203602.7 494282.9,203590.9 494299.4,203578.6 494316.9,203578.36 494317.24,203562.9 494338.9,203550.8 494332.5,203546.6 494330.4,203540.7 494326.7,203533.8 494322.3,203522.1 494315.4,203519.1 494313.4,203516.8 494311.9,203514.3 494309.8,203500.0 494297.28)))"  # noqa: E501
    expected = "MULTIPOLYGON (((-0.636258 51.722304,-0.636401 51.722227,-0.636567 51.722114,-0.636948 51.722313,-0.638848 51.723250,-0.638518 51.723485,-0.638483 51.723489,-0.638335 51.723462,-0.638198 51.723577,-0.638056 51.723680,-0.637821 51.723894,-0.637782 51.723920,-0.636525 51.723272,-0.635638 51.722863,-0.635766 51.722718,-0.636041 51.722453,-0.636258 51.722304)))"  # noqa: E501
    expected = "MULTIPOLYGON (((-0.636258 51.722304,-0.636041 51.722453,-0.635766 51.722718,-0.635638 51.722863,-0.636525 51.723272,-0.637782 51.723920,-0.637821 51.723894,-0.638056 51.723680,-0.638198 51.723577,-0.638335 51.723462,-0.638483 51.723489,-0.638518 51.723485,-0.638848 51.723250,-0.636948 51.722313,-0.636567 51.722114,-0.636401 51.722227,-0.636258 51.722304)))"  # noqa: E501
    assert wkt.normalise(value, issues=issues) == expected
    assert issue_type(issues) == "OSGB flipped"


def test_wkt_multipolygon_mercator():
    wkt = WktDataType()
    issues = IssueLog()
    value = "MULTIPOLYGON (((-7946.4687 6701859.138,-7925.9829 6701856.258,-7926.5076 6701852.447,-7946.5692 6701855.314,-7946.4687 6701859.138)))"  # noqa: E501
    expected = "MULTIPOLYGON (((-0.071384 51.453226,-0.071385 51.453205,-0.071205 51.453189,-0.071200 51.453210,-0.071384 51.453226)))"  # noqa: E501
    assert wkt.normalise(value, issues=issues) == expected
    assert issue_type(issues) == "Mercator"


def test_wkt_multipolygon_wgs84_winding():
    wkt = WktDataType()
    issues = IssueLog()
    # Buckinghamshire tree preservation zone
    value = "MULTIPOLYGON (((-0.111483 51.472552,-0.111481 51.472541,-0.111373 51.472542,-0.111372 51.472551,-0.111483 51.472552)),((-0.111483 51.472541,-0.111481 51.472541,-0.111481 51.472541,-0.111483 51.472541)))"  # noqa: E501
    expected = "MULTIPOLYGON (((-0.111483 51.472552,-0.111481 51.472541,-0.111373 51.472542,-0.111372 51.472551,-0.111483 51.472552)),((-0.111483 51.472541,-0.111481 51.472541,-0.111481 51.472541,-0.111483 51.472541)))"  # noqa: E501
    assert wkt.normalise(value, issues=issues) == expected
