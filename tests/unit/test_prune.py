#!/usr/bin/env -S pytest -s

from io import StringIO
import csv
from digital_land.phase.phase import Phase
from digital_land.pipeline import run_pipeline
from digital_land.phase.save import SavePhase
from digital_land.phase.prune import FactPrunePhase

test_data = [
    {"field": "name", "value": "Testy McTestFace"},
    {"field": "name", "value": ""},
    {"field": "end-date", "value": "1999-12-31"},
    {"field": "end-date", "value": ""},
]

expected_output = [
    test_data[0],
    test_data[2],
    test_data[3],
]


class TestPhase(Phase):
    def process(self, _):
        for block in test_data:
            yield {"row": block}


def TestPrunePipeline():
    output = StringIO()
    run_pipeline(TestPhase(), FactPrunePhase(), SavePhase(f=output))
    return output.getvalue()


def test_parse_pipeline():
    output = TestPrunePipeline()
    f = StringIO(output)
    index = 0
    for row in csv.DictReader(f, delimiter=","):
        assert row == expected_output[index]
        index += 1

    assert index == len(expected_output)
