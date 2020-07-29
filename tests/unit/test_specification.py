from digital_land.specification import Specification


def test_dataset_names():
    specification = Specification("tests/data/specification")
    assert specification.dataset_names == ["dataset-one", "dataset-two"]


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
    assert specification.field_names == ["field-one"]


def test_field():
    specification = Specification("tests/data/specification")
    assert specification.field["field-one"]["datatype"] == "string"


def test_schema_field():
    specification = Specification("tests/data/specification")
    assert specification.schema_field["schema-one"] == ["name"]
