#!/usr/bin/env -S pytest -s

from digital_land.specification import Specification
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase


def test_entity_reference():
    specification = Specification("./specification")
    phase = EntityReferencePhase(specification)

    row = {"prefix": "tree", "reference": "32"}
    phase.process_row(row, "tree")
    assert row["prefix"] == "tree"
    assert row["reference"] == "32"

    row = {}
    phase.process_row(row, "tree")
    assert row["prefix"] == "tree"
    assert row["reference"] == ""

    row = {"reference": "31"}
    phase.process_row(row, "tree")
    assert row["prefix"] == "tree"
    assert row["reference"] == "31"

    row = {"tree": "33"}
    phase.process_row(row, "tree")
    assert row["prefix"] == "tree"
    assert row["reference"] == "33"

    row = {"conservation-area": "CA04"}
    phase.process_row(row, "conservation-area")
    assert row["prefix"] == "conservation-area"
    assert row["reference"] == "CA04"

    row = {"organisation": "development-corporation:Q1234"}
    phase.process_row(row, "organisation")
    assert row["prefix"] == "development-corporation"
    assert row["reference"] == "Q1234"


def test_fact_reference():
    specification = Specification("./specification")
    phase = FactReferencePhase(specification)

    row = {"field": "organisation", "value": "development-corporation:Q1234"}
    phase.process_row(row, "organisation")
    assert row["prefix"] == "development-corporation"
    assert row["reference"] == "Q1234"

    row = {"field": "tree", "value": "33"}
    phase.process_row(row, "tree")
    assert row["prefix"] == "tree"
    assert row["reference"] == "33"

    row = {"field": "tree", "value": "33", "prefix": "wikidata", "reference": "Q9999"}
    phase.process_row(row, "tree")
    assert row["prefix"] == "wikidata"
    assert row["reference"] == "Q9999"
