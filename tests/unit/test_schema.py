from digital_land.schema import Schema


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
