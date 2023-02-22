#!/usr/bin/env -S pytest -svv
import shapely

from digital_land.log import IssueLog
from digital_land.phase.combine import combine_geometries, FactCombinePhase


def test_combine_geometries():
    expected_geometry = shapely.wkt.loads(
        "MULTIPOLYGON (((30 10, 10 20, 17 33, 10 40, 20 40, 40 40, 45 40, 36 28, 30 10)))"
    )

    combined_wkt = combine_geometries(
        [
            "MULTIPOLYGON (((30 20,45 40,10 40,30 20)),((15 5,40 10,10 20,5 10,15 5))))",
            "MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))",
        ],
        precision=0,
    )

    actual_geometry = shapely.wkt.loads(combined_wkt)

    assert actual_geometry.is_valid
    assert actual_geometry.geom_type == "MultiPolygon"
    assert actual_geometry.wkt == expected_geometry.wkt


def test_combine_geometries_handles_disjoint_geometries():
    expected_geometry = shapely.wkt.loads(
        "MULTIPOLYGON (((5 10, 10 20, 40 10, 15 5, 5 10)), ((10 40, 45 40, 30 20, 10 40)))"
    )  # noqa
    combined_wkt = combine_geometries(
        [
            "MULTIPOLYGON (((30 20,45 40,10 40,30 20)))",
            "MULTIPOLYGON (((15 5,40 10,10 20,5 10,15 5)))",
        ],
        precision=0,
    )

    actual_geometry = shapely.wkt.loads(combined_wkt)

    assert actual_geometry.is_valid
    assert actual_geometry.geom_type == "MultiPolygon"
    assert actual_geometry.wkt == expected_geometry.wkt


def test_combine_phase():
    def stream():
        n = 0
        for row in [
            {"entity": 1, "field": "notes", "value": "hello"},
            {"entity": 1, "field": "name", "value": "test"},
            {"entity": 1, "field": "notes", "value": "world"},
        ]:
            n += 1
            yield {
                "line-number": n + 1,
                "entry-number": n,
                "row": row,
            }

    phase = FactCombinePhase(fields={"notes": "-"}, issue_log=IssueLog())

    output = []
    for block in phase.process(stream()):
        output.append(block)

    assert output[0]["row"]["field"] == "name"
    assert output[0]["row"]["value"] == "test"
    assert output[1]["row"]["field"] == "notes"
    assert output[1]["row"]["value"] == "hello-world"
    assert output[2]["row"]["field"] == "notes"
    assert output[2]["row"]["value"] == "hello-world"
