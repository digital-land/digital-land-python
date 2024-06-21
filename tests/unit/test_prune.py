#!/usr/bin/env -S pytest -s

from io import StringIO
import csv
from digital_land.phase.phase import Phase
from digital_land.pipeline import run_pipeline
from digital_land.phase.save import SavePhase
from digital_land.phase.prune import FactPrunePhase

test_data = [
    {"row": {"value": "42", "end-date": "1999-12-31"}},
    {"row": {"value": "", "end-date": "1999-12-31"}},
    {"row": {"value": "42", "end-date": ""}},
    {"row": {"value": "", "end-date": ""}},
]

expected_output = [
    {"value": "42", "end-date": "1999-12-31"},
    {"end-date": "1999-12-31"},
    {"value": "42", "end-date": ""},
    {"": ""},
]


class TestPhase(Phase):
    def process(self, _):
        for block in test_data:
            yield block


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
