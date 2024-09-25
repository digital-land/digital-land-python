import pytest

from digital_land.specification import Specification
from digital_land.datatype.string import StringDataType
from digital_land.datatype.integer import IntegerDataType


def test_dataset_names():
    specification = Specification("tests/data/specification")
    assert specification.dataset_names == [
        "dataset-one",
        "dataset-two",
        "dataset-three",
    ]


def test_dataset():
    specification = Specification("tests/data/specification")
    assert specification.dataset["dataset-one"]["name"] == "First Dataset"
    assert specification.dataset["dataset-one"]["text"] == "Text of first dataset"


def test_schema_names():
    specification = Specification("tests/data/specification")
    assert specification.schema_names == [
        "schema-one",
        "schema-two",
        "schema-three",
        "fact",
        "fact-resource",
    ]


def test_schema():
    specification = Specification("tests/data/specification")
    assert specification.schema["schema-one"]["name"] == "First Schema"
    assert specification.schema["schema-one"]["description"] == "Description one"


def test_dataset_schema():
    specification = Specification("tests/data/specification")
    assert specification.dataset_schema["dataset-one"] == ["schema-one"]
    assert specification.dataset_schema["dataset-two"] == ["schema-one", "schema-two"]


def test_datatype_names():
    specification = Specification("tests/data/specification")
    assert specification.datatype_names == [
        "curie",
        "datetime",
        "decimal",
        "flag",
        "wkt",
        "hash",
        "integer",
        "json",
        "latitude",
        "longitude",
        "pattern",
        "string",
        "text",
        "url",
    ]


def test_datatype():
    specification = Specification("tests/data/specification")
    assert specification.datatype["integer"]["name"] == "Integer"


def test_field_names():
    specification = Specification("tests/data/specification")
    assert set(specification.field_names) == set(
        [
            "category",
            "text",
            "field-string",
            "field-integer",
            "field-other-integer",
            "field-old",
            "field-category",
            "field-categories",
            "name",
            "organisation",
            "address",
            "amount",
            "date",
            "schema-three",
            "entity",
            "line-number",
            "entry-number",
            "value",
            "start-date",
            "end-date",
            "entry-date",
            "resource",
            "field",
            "reference-entity",
            "fact",
            "geometry",
        ]
    )


def test_field():
    specification = Specification("tests/data/specification")
    assert specification.field["field-string"]["datatype"] == "string"


def test_schema_field():
    specification = Specification("tests/data/specification")
    assert specification.schema_field["schema-one"] == ["name"]


def test_current_fieldnames():
    specification = Specification("tests/data/specification")
    assert set(specification.current_fieldnames()) == set(
        [
            "entity",
            "category",
            "text",
            "field-string",
            "field-integer",
            "name",
            "organisation",
            "address",
            "amount",
            "date",
            "schema-three",
            "start-date",
            "entry-date",
            "entry-number",
            "end-date",
            "fact",
            "field",
            "reference-entity",
            "line-number",
            "resource",
            "value",
            "geometry",
        ]
    )


def test_field_type():
    specification = Specification("tests/data/specification")
    assert type(specification.field_type("field-string")) is StringDataType
    assert type(specification.field_type("field-integer")) is IntegerDataType


def test_field_typology():
    specification = Specification("tests/data/specification")
    assert specification.field_typology("field-string") == "text"
    assert specification.field_typology("field-category") == "category"
    assert specification.field_typology("field-categories") == "category"


def test_field_parent():
    specification = Specification("tests/data/specification")
    assert specification.field_parent("field-string") == "text"


def test_typology():
    specification = Specification("tests/data/specification")
    assert specification.typology["category"]["name"] == "Category"


def test_get_field_datatype_map_returns_correct_values(mocker):
    fields = {"child-test": {"datatype": "string"}, "parent-test": {"datatype": "url"}}

    mocker.patch("digital_land.specification.Specification.__init__", return_value=None)

    spec = Specification()
    spec.field = fields
    assert {
        "child-test": "string",
        "parent-test": "url",
    } == spec.get_field_datatype_map()


def test_get_field_typology_map_returns_correct_values(mocker):
    fields = {
        "child-test": {"parent-field": "parent-test"},
        "parent-test": {"parent-field": "parent-test"},
    }

    mocker.patch("digital_land.specification.Specification.__init__", return_value=None)

    spec = Specification()
    spec.field = fields
    assert {
        "child-test": "parent-test",
        "parent-test": "parent-test",
    } == spec.get_field_typology_map()


@pytest.mark.parametrize(
    "input,expected",
    [
        ({"field": {"prefix": "prefix"}}, {"field": "prefix"}),
        ({"field": {}}, {"field": "field"}),
        ({"field": {"prefix": ""}}, {"field": "field"}),
    ],
)
def test_get_field_prefix_map_returns_correct_values(input, expected):
    spec = Specification()
    spec.field = input
    assert expected == spec.get_field_prefix_map()
