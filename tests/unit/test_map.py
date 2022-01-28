#!/usr/bin/env py.test

from io import StringIO
import csv

from digital_land.phase.save import fsave
from digital_land.phase.map import MapPhase


class CustomReader(csv.DictReader):
    def __next__(self):
        row = super().__next__()
        return {"row": row, "resource": "dummy_resource"}


def _reader(s):
    return CustomReader(StringIO(s))


def test_map_headers():
    mapper = MapPhase(["One", "Two"])

    reader = _reader("one,Two\r\n1,2\r\n")

    assert reader.fieldnames == ["one", "Two"]
    assert mapper.headers(reader.fieldnames) == {"one": "One", "Two": "Two"}


def test_map_headers_normalisation():
    mapper = MapPhase(["One", "Two", "RequiresNormalisation"])

    reader = _reader("one,Two,requires normalisation\r\n1,2\r\n")

    assert reader.fieldnames == ["one", "Two", "requires normalisation"]
    assert mapper.headers(reader.fieldnames) == {
        "one": "One",
        "Two": "Two",
        "requires normalisation": "RequiresNormalisation",
    }


def test_map_headers_patch():
    column = {"oen": "One"}
    mapper = MapPhase(["One"], column)

    reader = _reader("oen,two\r\n1,2\r\n")

    assert mapper.headers(reader.fieldnames) == {"oen": "One"}


def test_map_headers_column_clash():
    column = {"une": "One", "ein": "One"}
    mapper = MapPhase(["One"], column)

    reader = _reader("une,ein\r\n1,2\r\n")

    assert mapper.headers(reader.fieldnames) == {"ein": "One", "une": "One"}


def test_map():
    fieldnames = ["One", "Two"]
    column = {
        "one": "One",
    }
    mapper = MapPhase(fieldnames, column)

    stream = _reader("one,Two,Three\r\n1,2,3\r\n")
    stream = mapper.process(stream)

    output = StringIO()
    fsave(stream, output, fieldnames=fieldnames)
    assert output.getvalue() == "One,Two\r\n1,2\r\n"


def test_map_concat():
    fieldnames = ["CombinedField"]
    concat = {"CombinedField": {"fields": ["part1", "part2"], "separator": "."}}
    mapper = MapPhase(fieldnames, concat=concat)

    stream = _reader("part1,part2\r\nfirst,second\r\n")
    stream = mapper.process(stream)

    output = StringIO()
    fsave(stream, output, fieldnames=fieldnames)
    assert output.getvalue() == "CombinedField\r\nfirst.second\r\n"
