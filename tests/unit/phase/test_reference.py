#!/usr/bin/env -S pytest -svv
import pytest


from digital_land.specification import Specification
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.reference import split_curie


def test_split_curie():
    assert ["", "value"] == split_curie("value")
    assert ["wikidata", "Q1234"] == split_curie("wikidata:Q1234")
    assert ["", "NSP22: A Street"] == split_curie("NSP22: A Street")
    assert ["", "Not A CURIE:"] == split_curie("Not A CURIE:")


def test_entity_reference():
    phase = EntityReferencePhase(dataset="tree", prefix="tree")

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
    field_typology_map = {
        "tree": "geography",
        "value": "value",
        "reference": "value",
        "prefix": "value",
        "conservation-area": "geography",
    }
    phase = FactReferencePhase(field_typology_map=field_typology_map)

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


def test_get_field_typology_name_works_with_field_typoogy_map():
    field_typology_map = {"reference": "value"}
    phase = FactReferencePhase(field_typology_map=field_typology_map)
    typology = phase.get_field_typology_name("reference")
    assert typology == "value"


def test_get_field_typology_name_riases_warning_for_spec():
    spec = Specification()
    spec.field = {
        "reference": {"parent-field": "value"},
        "value": {"parent-field": "value"},
    }
    phase = FactReferencePhase(specification=spec)
    with pytest.warns(DeprecationWarning):
        typology = phase.get_field_typology_name("reference")
    assert typology == "value"


def test_get_field_typology_name_raises_error_for_missing_mapping():
    field_typology_map = {}
    phase = FactReferencePhase(field_typology_map=field_typology_map)
    with pytest.raises(ValueError):
        phase.get_field_typology_name("reference")


def test_get_field_prefix_works_with_field_prefix_map():
    field_prefix_map = {"reference": "value"}
    phase = FactReferencePhase(field_prefix_map=field_prefix_map)
    typology = phase.get_field_prefix("reference")
    assert typology == "value"


def test_get_field_prefix_riases_warning_for_spec():
    spec = Specification()
    spec.field = {
        "conservation-area": {"prefix": "conservation-area"},
    }
    phase = FactReferencePhase(specification=spec)
    with pytest.warns(DeprecationWarning):
        prefix = phase.get_field_prefix("conservation-area")
    assert prefix == "conservation-area"


def test_get_field_prefix_uses_field_name_for_missing_mapping():
    field_prefix_map = {}
    phase = FactReferencePhase(field_prefix_map=field_prefix_map)

    prefix = phase.get_field_prefix("reference")
    assert prefix == "reference"
