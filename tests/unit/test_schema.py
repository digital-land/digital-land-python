from digital_land.schema import Schema
from digital_land.datatype.enum import EnumDataType


def test_schema_init():
    s = Schema("tests/data/schema.json")
    assert s.fieldnames == [
        "one",
        "two",
        "three",
    ]


def test_normalise_fieldname():
    s = Schema("tests/data/schema.json")
    assert s.normalise("one") == "one"
    assert s.normalise("One") == "one"
    assert s.normalise("A Field") == "afield"


def test_schema_typos():
    s = Schema("tests/data/schema-typos.json")
    typos = s.typos()
    assert typos == {
        "dos": "two",
        "due": "one",
        "one": "one",
        "thirdcolumn": "three",
        "three": "three",
        "two": "two",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
    }


def test_current_fieldnames():
    s = Schema("tests/data/schema-deprecated.json")
    assert s.current_fieldnames() == [
        "one",
        "two",
    ]


def test_required_fieldnames():
    s = Schema("tests/data/schema-required.json")
    assert s.required_fieldnames() == [
        "one",
    ]


def test_default_fieldnames():
    s = Schema("tests/data/schema-default.json")
    assert s.default_fieldnames() == {"two": ["one"]}


def test_strip():
    s = Schema("tests/data/schema-strip.json")

    assert s.strip("one", "no-op") == "no-op"
    assert s.strip("one", "data test-suffix") == "data"
    assert s.strip("two", "data test-suffix") == "data test-suffix"
    assert s.strip("one", "data test-suffix middle") == "data test-suffix middle"


def test_field_type():
    s = Schema("tests/data/schema-field-types.json")

    assert type(s.field_type("one")) == EnumDataType
    # TODO more field types
