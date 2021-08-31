import dataclasses

import pytest

from digital_land.model.fact import Fact


def test_fact():
    entity = 1
    slug = "/slug/one"
    attribute = "name"
    value = "First thing"

    fact = Fact(entity, slug, attribute, value)

    assert fact.entity == entity
    assert fact.slug == slug
    assert fact.attribute == attribute
    assert fact.value == value


def test_fact_is_immutable():
    entity = 1
    slug = "/slug/one"
    attribute = "name"
    value = "Current name"

    fact = Fact(entity, slug, attribute, value)

    with pytest.raises(
        dataclasses.FrozenInstanceError, match="^cannot assign to field"
    ):
        fact.value = "New name"


def test_fact_equality():
    entity = 1
    slug = "/slug/one"
    attribute = "name"
    value = "First thing"

    fact_1 = Fact(entity, slug, attribute, value)
    fact_2 = Fact(entity, slug, attribute, value)

    assert fact_1 == fact_2
