from .conftest import FakeDictReader
from digital_land.transform import Transformer


def test_transform():
    fields = {"field-one": "FieldOne"}
    t = Transformer(fields)
    reader = FakeDictReader([{"FieldOne": "123"}], "some-resource")

    output = list(t.transform(reader))

    assert output == [{"resource": "some-resource", "row": {"field-one": "123"}}]
