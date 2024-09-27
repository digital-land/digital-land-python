#!/usr/bin/env -S pytest -svv

from .testphase import TestPhase
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


def test_concat_prepend_append():
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
        ),
        "posX,posY\r\n1.02,2.04\r\n",
    )
    assert output == "point,posX,posY\r\nPOINT(1.02 2.04),1.02,2.04\r\n"
