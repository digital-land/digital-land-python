import re
from digital_land.normalise import Normaliser


def test_init():
    n = Normaliser()
    assert n.null_path.endswith("patch/null.csv")
    assert n.skip_patterns == []

    n = Normaliser([r"^,*[^,]*,*$"], null_path="tests/data/null.csv")
    assert n.null_path == "tests/data/null.csv"
    assert n.skip_patterns == [re.compile('^,*[^,]*,*$')]


def test_normalise_whitespace():
    n = Normaliser()
    assert n.normalise_whitespace(["a"]) == ["a"]
    assert n.normalise_whitespace(["a", "b"]) == ["a", "b"]
    assert n.normalise_whitespace(["a "]) == ["a"]
    assert n.normalise_whitespace([" a"]) == ["a"]
    assert n.normalise_whitespace([" a "]) == ["a"]
    assert n.normalise_whitespace(["a \n"]) == ["a"]


def test_strip_nulls():
    n = Normaliser(null_path="tests/data/null.csv")
    assert n.strip_nulls(["a", "b"]) == ["a", "b"]
    assert n.strip_nulls(["a", "????"]) == ["a", ""]
    assert n.strip_nulls(["a", "----"]) == ["a", ""]
    assert n.strip_nulls(["a", "null"]) == ["a", ""]
    assert n.strip_nulls(["a", "<null>"]) == ["a", ""]


def test_skip():
    n = Normaliser(["^Unnamed: 0,"])
    assert n.skip(["Unnamed: 0, "])
    assert not n.skip({"a": "b", "c": "d"})


def test_skip_blank_rows():
    n = Normaliser()
    assert list(
        n.normalise([{"line": ["1", "2"]}, {"line": ["", ""]}, {"line": ["3", "4"]}])
    ) == [{"line": ["1", "2"]}, {"line": ["3", "4"]}]
