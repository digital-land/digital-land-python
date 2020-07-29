from digital_land.pipeline import Pipeline


def test_typos():
    p = Pipeline("pipeline-one", "tests/data/")
    typos = p.columns()

    assert typos == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
    }


def test_resource_specific_typos():
    p = Pipeline("pipeline-one", "tests/data/")
    typos = p.columns("some-resource")

    assert (
        list(typos)[0] == "quatro"
    ), "resource specific typo 'quatro' should appear first in the returned dict"

    assert typos == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
        "quatro": "four",
    }
