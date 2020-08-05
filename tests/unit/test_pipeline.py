from digital_land.pipeline import Pipeline


def test_pipeline():
    p = Pipeline("tests/data/pipeline/")
    assert p.pipeline["pipeline-one"] == "schema-one"


def test_columns():
    p = Pipeline("tests/data/pipeline/")
    column = p.columns("pipeline-one")

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
    }


def test_resource_specific_columns():
    p = Pipeline("tests/data/pipeline/")
    column = p.columns("pipeline-one", "some-resource")

    assert (
        list(column)[0] == "quatro"
    ), "resource specific column 'quatro' should appear first in the returned dict"

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
        "quatro": "four",
    }


def test_skip_patterns():
    p = Pipeline("tests/data/pipeline/")
    pattern = p.skip_patterns("pipeline-one")
    assert "^Unnamed: 0," in pattern


def test_patches():
    p = Pipeline("tests/data/pipeline/")
    patches = p.patches("pipeline-one")
    assert patches == {"field-one": {"pat": "val"}}


def test_resource_specific_patches():
    p = Pipeline("tests/data/pipeline/")
    patches = p.patches("pipeline-one", "resource-one")
    assert patches == {"field-one": {"something": "else", "pat": "val"}}
