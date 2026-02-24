import polars as pl
import pytest
from digital_land.phase_polars.transform.parse import ParsePhase


@pytest.fixture
def parse_phase():
    return ParsePhase()


def test_parse_adds_entry_number_column(parse_phase):
    lf = pl.LazyFrame({"col1": ["a", "b", "c"], "col2": [1, 2, 3]})
    result = parse_phase.process(lf).collect()
    assert "entry-number" in result.columns
    assert result["entry-number"].to_list() == [1, 2, 3]


def test_parse_preserves_existing_columns(parse_phase):
    lf = pl.LazyFrame({"col1": ["a", "b"], "col2": [1, 2]})
    result = parse_phase.process(lf).collect()
    assert "col1" in result.columns
    assert "col2" in result.columns
    assert result["col1"].to_list() == ["a", "b"]
    assert result["col2"].to_list() == [1, 2]


def test_parse_empty_dataframe(parse_phase):
    lf = pl.LazyFrame({"col1": [], "col2": []})
    result = parse_phase.process(lf).collect()
    assert "entry-number" in result.columns
    assert len(result) == 0


def test_parse_single_row(parse_phase):
    lf = pl.LazyFrame({"col1": ["a"], "col2": [1]})
    result = parse_phase.process(lf).collect()
    assert result["entry-number"].to_list() == [1]


def test_parse_entry_number_starts_at_one(parse_phase):
    lf = pl.LazyFrame({"col1": ["a", "b", "c", "d", "e"]})
    result = parse_phase.process(lf).collect()
    assert result["entry-number"][0] == 1
    assert result["entry-number"][-1] == 5


def test_parse_returns_lazyframe(parse_phase):
    lf = pl.LazyFrame({"col1": ["a", "b"]})
    result = parse_phase.process(lf)
    assert isinstance(result, pl.LazyFrame)
