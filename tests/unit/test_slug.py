from digital_land.slug import Slugger

from .conftest import FakeDictReader


def test_generate_slug():
    row = {"organisation": "some:org", "identifier": "id"}
    assert (
        Slugger.generate_slug("prefix", "identifier", row, "organisation")
        == "/prefix/some/org/id"
    )


def test_generate_slug_missing_values():
    row = {"identifier": "CA01"}
    assert (
        Slugger.generate_slug("conservation-area", "identifier", row, "organisation")
        is None
    )


def test_generate_slug_replace_special_char():
    row = {"organisation": "some:org", "identifier": "id+1"}
    assert (
        Slugger.generate_slug("prefix", "identifier", row, "organisation")
        == "/prefix/some/org/id-1"
    )


def test_generate_slug_no_scope():
    row = {"organisation": "some:org", "identifier": "parish:E1234"}
    assert Slugger.generate_slug(None, "identifier", row) == "/parish/E1234"


def test_slug():
    s = Slugger("conservation-area", "conservation-area", "organisation")
    reader = FakeDictReader(
        [
            {
                "organisation": "local-authority-eng:YOR",
                "conservation-area": "conservation-area:CA01",
            },
        ]
    )
    output = list(s.slug(reader))
    assert len(output) == 1
    assert output[0]["row"]["slug"] == "/conservation-area/local-authority-eng/YOR/CA01"
