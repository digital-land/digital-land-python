#!/usr/bin/env -S pytest -svv

from .testphase import TestPhase
from digital_land.log import ColumnFieldLog
from digital_land.phase.concat import ConcatFieldPhase


def test_concat():
    output = TestPhase(
        ConcatFieldPhase(
            concats={
                "CombinedField": {
                    "fields": ["part1", "part2"],
                    "separator": ".",
                    "prepend": "",
                    "append": "",
                },
            },
        ),
        "part1,part2\r\nfirst,second\r\n",
    )
    assert output == "CombinedField,part1,part2\r\nfirst.second,first,second\r\n"


def test_concat_log():
    log = ColumnFieldLog()
    output = TestPhase(
        ConcatFieldPhase(
            concats={
                "CombinedField": {
                    "fields": ["part1", "part2"],
                    "separator": ".",
                    "prepend": "",
                    "append": "",
                },
            },
            log=log,
        ),
        "part1,part2\r\nfirst,second\r\nthird,fourth\r\n",
    )
    assert (
        output
        == "CombinedField,part1,part2\r\nfirst.second,first,second\r\nthird.fourth,third,fourth\r\n"
    )
    assert log.rows == [
        {
            "dataset": "",
            "resource": "",
            "column": "CombinedField",
            "field": "part1.part2",
        }
    ]


def test_concat_prepend_append():
    log = ColumnFieldLog()
    output = TestPhase(
        ConcatFieldPhase(
            concats={
                "point": {
                    "fields": ["posX", "posY"],
                    "separator": " ",
                    "prepend": "POINT(",
                    "append": ")",
                }
            },
            log=log,
        ),
        "posX,posY\r\n1.02,2.04\r\n3.06,4.08\r\n",
    )
    assert (
        output
        == "point,posX,posY\r\nPOINT(1.02 2.04),1.02,2.04\r\nPOINT(3.06 4.08),3.06,4.08\r\n"
    )
    assert log.rows == [
        {"dataset": "", "resource": "", "column": "point", "field": "POINT(posX posY)"}
    ]
