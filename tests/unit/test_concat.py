#!/usr/bin/env -S pytest -svv

from .testphase import TestPhase
from digital_land.phase.concat import ConcatFieldPhase


def test_map_concat():
    output = TestPhase(
        ConcatFieldPhase(
            concats={"CombinedField": {"fields": ["part1", "part2"], "separator": "."}},
        ),
        "part1,part2\r\nfirst,second\r\n",
    )
    assert output == "CombinedField,part1,part2\r\nfirst.second,first,second\r\n"
