import pytest

from digital_land.phase.harmonise import HarmonisePhase
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification
from digital_land.issues import Issues

from .conftest import FakeDictReader


def test_harmonise_field():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = HarmonisePhase(specification, pipeline)

    assert h.harmonise_field("field-string", None) == ""
    assert h.harmonise_field("field-string", "value") == "value"


def test_harmonise_apply_patch():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    issues = Issues()

    patch = {"field-string": {"WRONG": "right", "same": "same"}}

    h = HarmonisePhase(specification, pipeline, patch=patch, issues=issues)

    assert h.apply_patch("field-string", "right") == "right"
    assert h.apply_patch("field-string", "WRONG") == "right"
    assert h.apply_patch("field-string", "same") == "same"

    issue = issues.rows.pop()
    assert issue["field"] == "field-string"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "WRONG"
    assert issues.rows == []


def test_harmonise():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = HarmonisePhase(specification, pipeline)
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


# TODO reinstate this test
@pytest.mark.skip()
def test_harmonise_passes_resource():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = HarmonisePhase(specification, pipeline)
    reader = FakeDictReader([{"field-integer": "123"}], "some-resource")
    output = h.process(reader)
    assert next(output)["resource"] == "some-resource"


# TODO reinstate this test
@pytest.mark.skip()
def test_default():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = HarmonisePhase(specification, pipeline)
    reader = FakeDictReader(
        [
            {"field-integer": "", "field-other-integer": "123"},
            {"field-integer": "321", "field-other-integer": "123"},
        ],
        "resource-one",
    )
    output = list(h.process(reader))
    assert output[0]["row"]["field-integer"] == "123", "value is taken from default"
    assert output[1]["row"]["field-integer"] == "321", "value is not overridden"
