#!/usr/bin/env -S pytest -sv

from io import StringIO
from digital_land.pipeline import run_pipeline
from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.save import SavePhase


def TestPipeline(phase, inputs):
    output = StringIO()
    run_pipeline(
        LoadPhase(f=StringIO(inputs)), ParsePhase(), phase, SavePhase(f=output)
    )
    return output.getvalue()


def test_map_concat():
    c = ConcatFieldPhase(
        concats={"CombinedField": {"fields": ["part1", "part2"], "separator": "."}},
    )
    output = TestPipeline(c, "part1,part2\r\nfirst,second\r\n")
    assert output == "CombinedField\r\nfirst.second\r\n"
