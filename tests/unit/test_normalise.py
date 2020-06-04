from digital_land.normalise import Normaliser


def test_init():
    n = Normaliser()
    assert n.null_path.endswith("patch/null.csv")
    assert n.skip_path.endswith("patch/skip.csv")

    n = Normaliser(skip_path="tests/data/skip.csv", null_path="tests/data/null.csv")
    assert n.null_path == "tests/data/null.csv"
    assert n.skip_path == "tests/data/skip.csv"


def test_normalise_whitespace():
    n = Normaliser()
    assert n.normalise_whitespace(["a"]) == ["a"]
    assert n.normalise_whitespace(["a "]) == ["a"]
    assert n.normalise_whitespace([" a"]) == ["a"]
    assert n.normalise_whitespace([" a "]) == ["a"]
    assert n.normalise_whitespace(["a   "]) == ["a"]
    assert n.normalise_whitespace(["a  \n "]) == ["a"]


def test_strip_nulls():
    n = Normaliser(null_path="tests/data/null.csv")
    assert n.strip_nulls(["a"]) == ["a"]
    assert n.strip_nulls(["????"]) == [""]
    assert n.strip_nulls(["----"]) == [""]
    assert n.strip_nulls(["null"]) == [""]
    assert n.strip_nulls(["<null>"]) == [""]


def test_skip():
    n = Normaliser(skip_path="tests/data/skip.csv")
    assert n.skip(["Unnamed: 0", ""])
    assert not n.skip(["b", "d"])


def test_skip_blank_rows():
    n = Normaliser()
    assert list(
        n.normalise([["a"], [""], ["b"]])) == [["a"], ["b"]]
