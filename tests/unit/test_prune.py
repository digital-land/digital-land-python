#!/usr/bin/env -S pytest -s

from io import StringIO
from digital_land.phase.phase import Phase
from digital_land.pipeline import run_pipeline
from digital_land.phase.save import SavePhase
from digital_land.phase.prune import FactPrunePhase

test_data = [{"row": {"value": "42"}}, {"row": {"value": ""}}, {"row": {"value": None}}]


class TestPhase(Phase):
    def __init__(self):
        pass

    def process(self, _):
        for block in test_data:
            yield block


def TestPrunePipeline():
    output = StringIO()
    run_pipeline(TestPhase(), FactPrunePhase(), SavePhase(f=output))
    return output.getvalue()


def test_parse_pipeline():
    output = TestPrunePipeline()
    assert output.split() == ["value", "42", '""']
