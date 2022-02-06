#!/usr/bin/env py.test
import os
from digital_land.phase.convert import ConvertPhase


def test_load_xlsm():
    path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
    reader = ConvertPhase().process(path)
    block = next(reader)
    assert block["resource"] == "brentwood"
    assert block["line-number"] == 1
    assert "OrganisationURI" in block["line"]
