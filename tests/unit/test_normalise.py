import re
from digital_land.phase.normalise import NormalisePhase


def test_init():
    n = NormalisePhase()
    assert n.skip_patterns == []

    n = NormalisePhase([r"^,*[^,]*,*$"])
    assert n.skip_patterns == [re.compile("^,*[^,]*,*$")]


def test_normalise_whitespace():
    n = NormalisePhase()
    assert n.normalise_whitespace(["a"]) == ["a"]
    assert n.normalise_whitespace(["a", "b"]) == ["a", "b"]
    assert n.normalise_whitespace(["a "]) == ["a"]
    assert n.normalise_whitespace([" a"]) == ["a"]
    assert n.normalise_whitespace([" a "]) == ["a"]
    assert n.normalise_whitespace(["a \n"]) == ["a"]


def test_strip_nulls():
    n = NormalisePhase()
    assert n.strip_nulls(["a", "b"]) == ["a", "b"]
    assert n.strip_nulls(["a", "????"]) == ["a", ""]
    assert n.strip_nulls(["a", "----"]) == ["a", ""]
    assert n.strip_nulls(["a", "null"]) == ["a", ""]
    assert n.strip_nulls(["a", "<null>"]) == ["a", ""]


def test_skip():
    n = NormalisePhase(["^Unnamed: 0,"])
    assert n.skip(["Unnamed: 0, "])
    assert not n.skip({"a": "b", "c": "d"})


def test_skip_blank_rows():
    n = NormalisePhase()
    assert list(
        n.process([{"line": ["1", "2"]}, {"line": ["", ""]}, {"line": ["3", "4"]}])
    ) == [{"line": ["1", "2"]}, {"line": ["3", "4"]}]
