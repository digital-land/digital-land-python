from .conftest import FakeDictReader
from digital_land.specification import Specification
from digital_land.pipeline import Pipeline
from digital_land.harmonise import Harmoniser


def test_harmonise_field():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = Harmoniser(specification, pipeline)

    assert h.harmonise_field("field-string", None) == ""
    assert h.harmonise_field("field-string", "value") == "value"


def test_harmonise():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = Harmoniser(specification, pipeline)
    reader = FakeDictReader(
        [
            {"field-integer": "123"},
            {"field-integer": "  321   "},
            {"field-integer": "hello"},
        ]
    )
    output = list(h.harmonise(reader))
    assert len(output) == 3
    assert output[0]["row"] == {"field-integer": "123"}, "pass through valid data"
    assert output[1]["row"] == {"field-integer": "321"}, "whitespace trimmed"
    assert output[2]["row"] == {"field-integer": ""}, "remove bad data"


def test_harmonise_passes_resource():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = Harmoniser(specification, pipeline)
    reader = FakeDictReader([{"field-integer": "123"}], "some-resource")
    output = h.harmonise(reader)
    assert next(output)["resource"] == "some-resource"


def test_default():
    specification = Specification("tests/data/specification")
    pipeline = Pipeline("tests/data/pipeline", "pipeline-one")
    h = Harmoniser(specification, pipeline)
    reader = FakeDictReader(
        [
            {"field-integer": "", "field-other-integer": "123"},
            {"field-integer": "321", "field-other-integer": "123"},
        ],
        "resource-one",
    )
    output = list(h.harmonise(reader))
    assert output[0]["row"]["field-integer"] == "123", "value is taken from default"
    assert output[1]["row"]["field-integer"] == "321", "value is not overridden"
