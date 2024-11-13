#!/usr/bin/env -S py.test -svv
import pytest

from digital_land.phase.harmonise import HarmonisePhase
from digital_land.specification import Specification
from digital_land.log import IssueLog

from ..conftest import FakeDictReader


def test_harmonise_field():
    field_datatype_map = {"field-string": "string"}
    issues = IssueLog()

    h = HarmonisePhase(field_datatype_map=field_datatype_map, issues=issues)

    assert h.harmonise_field("field-string", None) == ""
    assert h.harmonise_field("field-string", "value") == "value"


def test_harmonise():
    field_datatype_map = {"field-integer": "integer"}
    issues = IssueLog()

    h = HarmonisePhase(field_datatype_map=field_datatype_map, issues=issues)
    reader = FakeDictReader(
        [
            {"field-integer": "123"},
            {"field-integer": "  321   "},
            {"field-integer": "hello"},
        ]
    )
    output = list(h.process(reader))
    assert len(output) == 3
    assert output[0]["row"] == {"field-integer": "123"}, "pass through valid data"
    assert output[1]["row"] == {"field-integer": "321"}, "whitespace trimmed"
    assert output[2]["row"] == {"field-integer": ""}, "remove bad data"


def test_harmonise_geometry_and_point_missing():
    field_datatye_map = {
        "geometry": "multipolygon",
        "point": "point",
        "organisation": "string",
    }
    issues = IssueLog()

    h = HarmonisePhase(field_datatype_map=field_datatye_map, issues=issues)
    reader = FakeDictReader(
        [
            {"geometry": "", "point": "", "organisation": "test_org"},
        ]
    )
    output = list(h.process(reader))

    assert len(output) == 1
    assert len(issues.rows) == 2

    # As both point and geometry are empty, there should be an issue logged for the empty "point" and "geometry" field
    for issue in issues.rows:
        assert issue["field"] in ["geometry", "point"]
        assert issue["issue-type"] == "missing value"
        assert issue["value"] == ""


def test_harmonise_geometry_present_point_missing():
    field_datatye_map = {
        "geometry": "multipolygon",
        "point": "point",
        "organisation": "string",
    }
    issues = IssueLog()

    h = HarmonisePhase(field_datatype_map=field_datatye_map, issues=issues)
    reader = FakeDictReader(
        [
            {
                "geometry": "MULTIPOLYGON (((-0.469666 51.801822, -0.469666 51.80819, -0.455246 51.80819, -0.455246 51.801822, -0.469666 51.801822)))",
                "point": "",
                "organisation": "test_org",
            },
        ]
    )
    output = list(h.process(reader))

    assert len(output) == 1

    # As one of geometry or point exist, no issue is flagged
    assert len(issues.rows) == 0


# TODO Why is the specification being read in here should we remove it?
def test_harmonise_geometry_present_no_point_field():
    specification = Specification("tests/data/specification")
    issues = IssueLog()

    h = HarmonisePhase(
        specification.get_field_datatype_map(), issues=issues, dataset="tree"
    )
    reader = FakeDictReader(
        [
            {
                "geometry": "MULTIPOLYGON (((-0.469666 51.801822, -0.469666 51.80819, -0.455246 51.80819, -0.455246 51.801822, -0.469666 51.801822)))",
                "organisation": "test_org",
            },
        ],
    )
    output = list(h.process(reader))

    assert len(output) == 1

    # As geometry is given (even without point field present) there is no issue raised
    assert len(issues.rows) == 0


def test_harmonise_missing_mandatory_values():
    issues = IssueLog()
    field_datatype_map = {
        "reference": "string",
        "name": "string",
        "description": "string",
        "document-url": "url",
        "documentation-url": "url",
        "organisation": "string",
    }

    h = HarmonisePhase(
        field_datatype_map=field_datatype_map,
        issues=issues,
        dataset="article-4-direction",
    )
    reader = FakeDictReader(
        [
            {
                "reference": "",
                "name": "A nice name",
                "description": "",
                "document-url": "",
                "documentation-url": "",
                "organisation": "test_org",
            },
        ],
    )
    output = list(h.process(reader))

    assert len(output) == 1
    assert len(issues.rows) == 4

    # It should have an issue logged for the empty mandatory fields except name for article-4-direction
    for issue in issues.rows:
        assert issue["field"] in [
            "reference",
            "description",
            "document-url",
            "documentation-url",
        ]
        assert issue["issue-type"] == "missing value"
        assert issue["value"] == ""


def test_get_field_datatype_name_uses_field_datatype_map():
    field_datatype_map = {"reference": "string"}
    phase = HarmonisePhase(field_datatype_map=field_datatype_map)
    datatype_name = phase.get_field_datatype_name("reference")
    assert datatype_name == "string"


def test_get_field_datatype_name_raises_error_for_missing_mapping():
    field_datatype_map = {}
    phase = HarmonisePhase(field_datatype_map=field_datatype_map)
    with pytest.raises(ValueError):
        phase.get_field_datatype_name("reference")


def test_validate_categorical_fields():
    specification = Specification("tests/data/specification")

    issues = IssueLog()

    h = HarmonisePhase(
        specification.get_field_datatype_map(),
        issues=issues,
        dataset="tree-preservation-zone",
        valid_category_values={
            "tree-preservation-zone-type": ["area", "group", "woodland"]
        },
    )

    reader = FakeDictReader(
        [
            {
                "reference": "1",
                "name": "Test TPO 1",
                "description": "Test",
                "tree-preservation-zone-type": "area",
            },
            {
                "reference": "2",
                "name": "Test TPO 2",
                "description": "Test",
                "tree-preservation-zone-type": "other",
            },
            {
                "reference": "3",
                "name": "Test TPO 3",
                "description": "Test",
                "tree-preservation-zone-type": "",
            },
        ],
    )

    output = list(h.process(reader))

    assert len(output) == 3
    # check the fields are set in the output
    assert output[0]["row"]["tree-preservation-zone-type"] == "area"
    assert output[1]["row"]["tree-preservation-zone-type"] == "other"
    assert output[2]["row"]["tree-preservation-zone-type"] == ""


    assert len(issues.rows) == 1
    # but we get an issue generated
    assert issues.rows[0]["issue-type"] == "invalid category value"


def test_validate_categorical_field_dataset():
    specification = Specification("tests/data/specification")

    issues = IssueLog()

    h = HarmonisePhase(
        specification.get_field_datatype_map(),
        issues=issues,
        dataset="conservation-area-document",
        valid_category_values={
            "document-type": ["area-appraisal", "notice", "designa", "area-map"]
        },
    )

    reader = FakeDictReader(
        [
            {
                "reference": "1",
                "document-type": "notice",
            },
            {
                "reference": "2",
                "document-type": "other",
            },
        ],
    )

    output = list(h.process(reader))

    assert len(output) == 2
    # check the fields are set in the output
    assert output[0]["row"]["document-type"] == "notice"
    assert output[1]["row"]["document-type"] == "other"

    assert len(issues.rows) == 1
    # but we get an issue generated
    assert issues.rows[0]["issue-type"] == "invalid category value"
