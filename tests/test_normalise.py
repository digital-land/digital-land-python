from digital_land.normalise import Normaliser


def test_init():
    n = Normaliser()
    assert n.null_path.endswith("patch/null.csv")
    assert n.skip_path.endswith("patch/skip.csv")

    n = Normaliser(skip_path="tests/data/skip.csv", null_path="tests/data/null.csv")
    assert n.null_path == "tests/data/null.csv"
    assert n.skip_path == "tests/data/skip.csv"
