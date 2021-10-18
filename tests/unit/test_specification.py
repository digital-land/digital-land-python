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
    assert specification.schema_names == ["schema-one", "schema-two", "schema-three"]


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
    assert specification.field_names == [
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
    ]


def test_field():
    specification = Specification("tests/data/specification")
    assert specification.field["field-string"]["datatype"] == "string"


def test_schema_field():
    specification = Specification("tests/data/specification")
    assert specification.schema_field["schema-one"] == ["name"]


def test_current_fieldnames():
    specification = Specification("tests/data/specification")
    assert specification.current_fieldnames() == [
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
        "slug",
    ]


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
