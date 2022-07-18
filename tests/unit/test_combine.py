#!/usr/bin/env -S pytest -svv

from digital_land.log import IssueLog
from digital_land.phase.combine import combine_geometries, FactCombinePhase


def test_combine_geometries():
    assert (
        combine_geometries(
            [
                "MULTIPOLYGON (((30 20,45 40,10 40,30 20)),((15 5,40 10,10 20,5 10,15 5))))",
                "MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))",
            ]
        )
        == "MULTIPOLYGON (((30 20,45 40,10 40,30 20)),((15 5,40 10,10 20,5 10,15 5))),((30 10, 40 40, 20 40, 10 20, 30 10)))"
    )


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
