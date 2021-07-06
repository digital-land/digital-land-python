from io import StringIO
import csv

from digital_land.filter import Filterer


class CustomReader(csv.DictReader):
    def __next__(self):
        row = super().__next__()
        return {"row": row, "resource": "dummy_resource"}


def _reader(s):
    return CustomReader(StringIO(s))


def test_filter():
    test_filter_patterns = {"three-column": "^[SE]"}

    filterer = Filterer(filter_patterns=test_filter_patterns)
    stream = _reader(
        "one-column,two-column,three-column\r\nOne,Two,Three\r\nFour,Five,Six\r\nSeven,Eight,Nine"
    )
    result = list(filterer.filter(stream))
    expected_result = [
        {
            "resource": "dummy_resource",
            "row": {"one-column": "One", "two-column": "Two", "three-column": "Three"},
        },
        {
            "resource": "dummy_resource",
            "row": {
                "one-column": "Seven",
                "two-column": "Eight",
                "three-column": "Nine",
            },
        },
    ]
    assert result == expected_result
