from io import StringIO
import csv

from digital_land.save import fsave
from digital_land.map import Mapper
from digital_land.schema import Schema


class CustomReader(csv.DictReader):
    def __next__(self):
        row = super().__next__()
        return {"row": row, "resource": "dummy_resource"}


def _reader(s):
    return CustomReader(StringIO(s))


def test_map_headers():
    schema = Schema("tests/data/schema.json")
    mapper = Mapper(schema)

    reader = _reader("one,two\r\n1,2\r\n")
    assert reader.fieldnames == ["one", "two"]

    assert mapper.headers(reader) == {"one": "one", "two": "two"}


def test_map():
    schema = Schema("tests/data/schema.json")
    mapper = Mapper(schema)

    stream = _reader("one,two\r\n1,2\r\n")

    assert mapper.headers(stream) == {"one": "one", "two": "two"}

    stream = mapper.map(stream)

    output = StringIO()
    fsave(stream, output, fieldnames=schema.fieldnames)
    assert output.getvalue() == "one,two,three\r\n1,2,\r\n"
