from digital_land.transform import Transformer

from .conftest import FakeDictReader


def test_transform_passthrough():
    fields = ["field-one"]
    transformations = {"field-one": "FieldOne"}
    t = Transformer(fields, transformations)
    reader = FakeDictReader([{"field-one": "123"}], "some-resource")

    output = list(t.transform(reader))

    assert output == [{"resource": "some-resource", "row": {"field-one": "123"}}]


def test_transform_transformation():
    fields = ["field-one"]
    transformations = {"field-one": "FieldOne"}
    t = Transformer(fields, transformations)
    reader = FakeDictReader([{"FieldOne": "123"}], "some-resource")

    output = list(t.transform(reader))

    assert output == [{"resource": "some-resource", "row": {"field-one": "123"}}]


def test_transform_point():
    fields = ["point"]
    t = Transformer(fields, {})
    reader = FakeDictReader(
        [{"GeoX": "-2.218153", "GeoY": "50.747808"}], "some-resource"
    )

    output = list(t.transform(reader))

    assert output == [
        {"resource": "some-resource", "row": {"point": "POINT(-2.218153 50.747808)"}}
    ]
