#!/usr/bin/env python3
import pytest
import polars as pl
from digital_land.phase_polars.transform.map import MapPhase, normalise


def test_headers_empty_columns():
    lf = pl.LazyFrame({"one": [1], "two": [2]})
    m = MapPhase(["one", "two"])
    result = m.process(lf).collect()
    assert result.columns == ["one", "two"]
    assert result.to_dicts() == [{"one": 1, "two": 2}]


def test_map_headers():
    lf = pl.LazyFrame({"one": [1], "THREE": [3]})
    m = MapPhase(["one", "two"], columns={"three": "two"})
    result = m.process(lf).collect()
    assert result.columns == ["one", "two"]
    assert result.to_dicts() == [{"one": 1, "two": 3}]


def test_map_straight():
    lf = pl.LazyFrame({"one": [1], "two": [2]})
    m = MapPhase(["one", "two"])
    result = m.process(lf).collect()
    assert result.columns == ["one", "two"]
    assert result.to_dicts() == [{"one": 1, "two": 2}]


@pytest.mark.parametrize(
    "column_name, expected",
    [
        ("hello_world", "hello-world"),
        ("hello-world", "hello-world"),
        ("Hello_World", "hello-world"),
        ("Hello-World", "hello-world"),
    ],
)
def test_map_normalize_removes_underscores(column_name, expected):
    actual = normalise(column_name)
    assert actual == expected


def test_map_column_names_with_underscores_when_column_in_specification():
    lf = pl.LazyFrame(
        {
            "Organisation_Label": ["col-1-val"],
            "end_date": ["col-2-val"],
            "SiteNameAddress": [""],
        }
    )

    fieldnames = ["Organisation_Label", "end_date", "SiteNameAddress"]
    columns = {
        "organisation-label": "Organisation-Label",
        "end-date": "end-date",
        "ownership": "OwnershipStatus",
    }

    m = MapPhase(fieldnames, columns)
    result = m.process(lf).collect()

    assert set(result.columns) == {"Organisation-Label", "SiteNameAddress", "end-date"}


def test_ignore_column():
    lf = pl.LazyFrame({"one": [1], "two": [2], "three": [3]})
    m = MapPhase(["one", "two"], columns={"three": "IGNORE"})
    result = m.process(lf).collect()
    assert result.columns == ["one", "two"]
    assert result.to_dicts() == [{"one": 1, "two": 2}]
