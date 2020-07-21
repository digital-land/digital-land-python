from digital_land.pipeline import Pipeline


def test_typos():
    p = Pipeline("test-pipeline", "tests/data/")
    typos = p.column_typos()

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
    p = Pipeline("test-pipeline", "tests/data/")
    typos = p.column_typos("some-resource")

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
