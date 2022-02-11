#!/usr/bin/env -S py.test -svv

from digital_land.phase.migrate import MigratePhase

from .conftest import FakeDictReader


def test_migrate_passthrough():
    fields = ["field-one"]
    migrateations = {"field-one": "FieldOne"}
    t = MigratePhase(fields, migrateations)
    reader = FakeDictReader([{"field-one": "123"}], "some-resource")

    output = list(t.process(reader))

    assert output == [{"resource": "some-resource", "row": {"field-one": "123"}}]


def test_migrate_migrateation():
    fields = ["field-one"]
    migrateations = {"field-one": "FieldOne"}
    t = MigratePhase(fields, migrateations)
    reader = FakeDictReader([{"FieldOne": "123"}], "some-resource")

    output = list(t.process(reader))

    assert output == [{"resource": "some-resource", "row": {"field-one": "123"}}]


def test_migrate_point():
    fields = ["point"]
    t = MigratePhase(fields, {})
    reader = FakeDictReader(
        [{"GeoX": "-2.218153", "GeoY": "50.747808"}], "some-resource"
    )

    output = list(t.process(reader))

    assert output[0]["row"] == {
        "point": "POINT(-2.218153 50.747808)",
    }
