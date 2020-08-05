from io import StringIO
import csv

from digital_land.save import fsave
from digital_land.map import Mapper


class CustomReader(csv.DictReader):
    def __next__(self):
        row = super().__next__()
        return {"row": row, "resource": "dummy_resource"}


def _reader(s):
    return CustomReader(StringIO(s))


def test_map_headers():
    column = {
        "one": "One",
        "Two": "Two",
    }
    mapper = Mapper([], column)

    reader = _reader("one,Two\r\n1,2\r\n")

    assert reader.fieldnames == ["one", "Two"]
    assert mapper.headers(reader.fieldnames) == {"one": "One", "Two": "Two"}


def test_map():
    fieldnames = ["One", "Two"]
    column = {
        "one": "One",
    }
    mapper = Mapper(fieldnames, column)

    stream = _reader("one,Two,Three\r\n1,2,3\r\n")
    stream = mapper.map(stream)

    output = StringIO()
    fsave(stream, output, fieldnames=fieldnames)
    assert output.getvalue() == "One,Two\r\n1,2\r\n"
