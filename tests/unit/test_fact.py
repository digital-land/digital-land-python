import dataclasses

import pytest

from digital_land.model.fact import Fact


def test_fact():
    entity = "/slug/one"
    attribute = "name"
    value = "First thing"

    fact = Fact(entity, attribute, value)

    assert fact.entity == entity
    assert fact.attribute == attribute
    assert fact.value == value


def test_fact_is_immutable():
    entity = "/slug/one"
    attribute = "name"
    value = "Current name"

    fact = Fact(entity, attribute, value)

    with pytest.raises(
        dataclasses.FrozenInstanceError, match="^cannot assign to field"
    ):
        fact.value = "New name"


def test_fact_equality():
    entity = "/slug/one"
    attribute = "name"
    value = "First thing"

    fact_1 = Fact(entity, attribute, value)
    fact_2 = Fact(entity, attribute, value)

    assert fact_1 == fact_2
