#!/usr/bin/env -S py.test -svv

from io import StringIO
import csv

from digital_land.phase.filter import FilterPhase


class CustomReader(csv.DictReader):
    def __next__(self):
        row = super().__next__()
        return {"row": row, "resource": "dummy_resource"}


def _reader(s):
    return CustomReader(StringIO(s))


def test_filter_in():
    patterns = {"name": "^T"}

    phase = FilterPhase(filters=patterns)
    stream = _reader("reference,name\r\n" + "1,One\r\n" + "2,Two\r\n" + "3,Three\r\n")
    out = list(phase.process(stream))
    assert out[0]["row"] == {"reference": "2", "name": "Two"}
    assert out[1]["row"] == {"reference": "3", "name": "Three"}
    assert len(out) == 2
