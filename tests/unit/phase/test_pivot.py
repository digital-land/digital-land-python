#!/usr/bin/env -S pytest -svv
from copy import deepcopy

from digital_land.phase.pivot import PivotPhase
from digital_land.log import IssueLog


def test_pivot():
    input = [
        {
            "priority": 1,
            "entity": "1234",
            "resource": "res0123",
            "line-number": 1,
            "entry-number": 1,
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
            "entry-number": 1,
            "line-number": 1,
            "priority": 1,
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": 1,
                "fact": "",
                "field": "entry-date",
                "line-number": 1,
                "priority": 1,
                "resource": "res0123",
                "value": "2024-12-04",
            },
        },
        {
            "entity": "1234",
            "entry-number": 1,
            "line-number": 1,
            "priority": 1,
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": 1,
                "fact": "",
                "field": "test-field",
                "line-number": 1,
                "priority": 1,
                "resource": "res0123",
                "value": "test-value",
            },
        },
    ]


def test_skips_items_in_issue_log():
    issue_log = IssueLog()
    issue_log.log_issue(
        "geometry", "invalid geometry", "", "The geometry is invalid", 1, 1, "1234"
    )

    input = [
        {
            "priority": 2,
            "entity": 1234,
            "resource": "res0123",
            "line-number": 1,
            "entry-number": 1,
            "row": {
                "entry-date": "2024-12-04",
                "geometry": "POINT (0 0)",
                "name": "Brownfield-on-Sea",
            },
        },
    ]

    output = [
        deepcopy(block) for block in PivotPhase(issue_log=issue_log).process(input)
    ]

    assert output == [
        {
            "entity": 1234,
            "entry-number": 1,
            "line-number": 1,
            "priority": 2,
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": 1,
                "fact": "",
                "field": "entry-date",
                "line-number": 1,
                "priority": 2,
                "resource": "res0123",
                "value": "2024-12-04",
            },
        },
        {
            "entity": 1234,
            "entry-number": 1,
            "line-number": 1,
            "priority": 2,
            "resource": "res0123",
            "row": {
                "entity": "",
                "entry-date": "2024-12-04",
                "entry-number": 1,
                "fact": "",
                "field": "name",
                "line-number": 1,
                "priority": 2,
                "resource": "res0123",
                "value": "Brownfield-on-Sea",
            },
        },
    ]
