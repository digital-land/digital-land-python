#!/usr/bin/env -S pytest -svv

from digital_land.specification import Specification
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.reference import split_curie


def test_split_curie():
    assert ["", "value"] == split_curie("value")
    assert ["wikidata", "Q1234"] == split_curie("wikidata:Q1234")
    assert ["", "NSP22: A Street"] == split_curie("NSP22: A Street")
    assert ["", "Not A CURIE:"] == split_curie("Not A CURIE:")


def test_entity_reference():
    specification = Specification("./specification")
    phase = EntityReferencePhase(dataset="tree", specification=specification)

    assert ("tree", "31") == phase.process_row({"reference": "31"})
    assert ("foo", "Q1234") == phase.process_row(
        {"prefix": "foo", "reference": "Q1234"}
    )
    assert ("wikidata", "Q1234") == phase.process_row({"reference": "wikidata:Q1234"})
    assert ("tree", "NSP22: Not A CURIE") == phase.process_row(
        {"reference": "NSP22: Not A CURIE"}
    )
    assert ("tree", "Not A CURIE:") == phase.process_row({"reference": "Not A CURIE:"})
    assert ("foo", "Q1234") == phase.process_row(
        {"prefix": "foo", "reference": "wikidata:Q1234"}
    )


def test_fact_reference():
    specification = Specification("./specification")
    phase = FactReferencePhase(dataset="tree", specification=specification)

    assert ("tree", "Q1234") == phase.process_row({"field": "tree", "value": "Q1234"})
    assert ("wikidata", "Q1234") == phase.process_row(
        {"field": "tree", "value": "wikidata:Q1234"}
    )
    assert ("foo", "Q1234") == phase.process_row(
        {"prefix": "foo", "reference": "Q1234", "field": "tree", "value": "Q1234"}
    )
    assert ("conservation-area", "CA01") == phase.process_row(
        {"field": "conservation-area", "value": "CA01"}
    )
