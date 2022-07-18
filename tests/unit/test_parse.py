#!/usr/bin/env -S pytest -s

from io import StringIO
from digital_land.pipeline import run_pipeline
from digital_land.phase.load import LoadPhase
from digital_land.phase.save import SavePhase
from digital_land.phase.parse import ParsePhase


def TestParsePipeline(inputs):
    output = StringIO()
    run_pipeline(LoadPhase(f=StringIO(inputs)), ParsePhase(), SavePhase(f=output))
    return output.getvalue()


def test_parse_pipeline():
    inputs = "One,Two\r\n1,2\r\n"
    output = TestParsePipeline(inputs)
    assert output == inputs
