#!/usr/bin/env -S pytest -svv

from io import StringIO
from digital_land.pipeline import run_pipeline
from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.map import MapPhase
from digital_land.phase.save import SavePhase


def TestPipeline(phase, inputs):
    output = StringIO()
    run_pipeline(
        LoadPhase(f=StringIO(inputs)), ParsePhase(), phase, SavePhase(f=output)
    )
    return output.getvalue()


def test_headers_empty_columns():
    m = MapPhase(["one", "two"])
    output = TestPipeline(m, "one,two\r\n1,2\r\n")
    assert output == ("one,two\r\n" "1,2\r\n")


def test_map_headers():
    m = MapPhase(["one", "two"], columns={"three": "two"})
    output = TestPipeline(m, "one,THREE\r\n1,3\r\n")
    assert output == "one,two\r\n1,3\r\n"


def test_map_straight():
    output = TestPipeline(MapPhase(["one", "two"]), "one,two\r\n1,2\r\n")
    assert output == "one,two\r\n1,2\r\n"


def test_map_headers_column_clash():
    m = MapPhase(["One"], {"une": "One", "ein": "One"})
    output = TestPipeline(m, "une,ein\r\n1,2\r\n")
    assert output == "One\r\n1\r\n"
