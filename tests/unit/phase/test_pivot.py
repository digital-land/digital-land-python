#!/usr/bin/env -S pytest -svv
from copy import deepcopy

from digital_land.phase.pivot import PivotPhase


def test_pivot():
    input = [
        {
            "priority": "1",
            "entity": "1234",
            "resource": "res0123",
            "line-number": "1",
            "entry-number": "1",
            "row": {
                "entry-date": "2024-12-04",
                "test-field": "test-value",
            },
        }
    ]

    output = [deepcopy(block) for block in PivotPhase().process(input)]

    assert output == [
        {
            "entity": "1234",
            "entry-number": "1",
            "line-number": "1",
            "priority": "1",
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": "1",
                "fact": "",
                "field": "entry-date",
                "line-number": "1",
                "priority": "1",
                "resource": "res0123",
                "value": "2024-12-04",
            },
        },
        {
            "entity": "1234",
            "entry-number": "1",
            "line-number": "1",
            "priority": "1",
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": "1",
                "fact": "",
                "field": "test-field",
                "line-number": "1",
                "priority": "1",
                "resource": "res0123",
                "value": "test-value",
            },
        },
    ]
