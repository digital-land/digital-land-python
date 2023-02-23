import shapely.wkt
from shapely.validation import explain_validity

from digital_land.datatype.wkt import WktDataType
from digital_land.log import IssueLog


def test_normalise_returns_valid_shape():
    input_wkt = "MULTIPOLYGON (((460316.9266 298735.6545,460316.9266 298731.3933,460311.2699 298731.127,460311.9181 298731.223,460304.7879 298735.6593,460304.7854 298735.6545,460316.9266 298735.6545)))"  # noqa: E501
    issue_log = IssueLog()
    output_wkt = WktDataType().normalise(input_wkt, issues=issue_log)
    output_geometry = shapely.wkt.loads(output_wkt)
    assert output_geometry.is_valid, explain_validity(output_geometry)


def test_normalise_returns_empty_geometry():
    input_wkt = "MULTIPOLYGON (((471419.04 240369.58,471418.996 240371.622,471419.0 240371.6,471419.04 240369.58)))"  # noqa: E501
    issue_log = IssueLog()
    output_wkt = WktDataType().normalise(input_wkt, issues=issue_log)
    assert output_wkt == ""
