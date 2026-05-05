import polars as pl
from digital_land.phase_polars.transform.normalise import NormalisePhase


def test_normalise_whitespace():
    """Test whitespace normalisation."""
    phase = NormalisePhase()

    lf = pl.DataFrame(
        {
            "field1": ["  value1  ", "\tvalue2\t", "value3\n"],
            "field2": ["  test  ", "data\r\n", "row3"],
        }
    ).lazy()

    result = phase.process(lf).collect()

    assert result["field1"][0] == "value1"
    assert result["field1"][1] == "value2"
    assert result["field1"][2] == "value3"
    assert result["field2"][0] == "test"


def test_strip_nulls():
    """Test null pattern stripping."""
    phase = NormalisePhase()

    lf = pl.DataFrame(
        {
            "field1": ["value1", "NULL", "n/a", "???"],
            "field2": ["test", "---", "N/A", "data"],
        }
    ).lazy()

    result = phase.process(lf).collect()

    assert result["field1"][0] == "value1"
    assert result["field1"][1] == ""
    assert result["field2"][0] == "test"


def test_filter_blank_rows():
    """Test filtering of blank rows."""
    phase = NormalisePhase()

    lf = pl.DataFrame(
        {"field1": ["value1", "", "value3"], "field2": ["test", "", "data"]}
    ).lazy()

    result = phase.process(lf).collect()

    assert len(result) == 2
    assert result["field1"][0] == "value1"
    assert result["field1"][1] == "value3"


def test_skip_patterns():
    """Test skip patterns."""
    phase = NormalisePhase(skip_patterns=["^SKIP.*"])

    lf = pl.DataFrame(
        {"field1": ["value1", "SKIP_THIS", "value3"], "field2": ["test", "row", "data"]}
    ).lazy()

    result = phase.process(lf).collect()

    assert len(result) == 2
    assert result["field1"][0] == "value1"
    assert result["field1"][1] == "value3"


def test_empty_dataframe():
    """Test processing empty dataframe."""
    phase = NormalisePhase()

    lf = pl.DataFrame({"field1": [], "field2": []}).lazy()

    result = phase.process(lf).collect()

    assert len(result) == 0


def test_newline_conversion():
    """Test newline to CRLF conversion."""
    phase = NormalisePhase()

    lf = pl.DataFrame({"field1": ["line1\nline2", "line1\r\nline2"]}).lazy()

    result = phase.process(lf).collect()

    assert result["field1"][0] == "line1\r\nline2"
    assert result["field1"][1] == "line1\r\nline2"


def test_multiple_skip_patterns():
    """Test multiple skip patterns."""
    phase = NormalisePhase(skip_patterns=["^SKIP", "^IGNORE"])

    lf = pl.DataFrame({"field1": ["keep", "SKIP_THIS", "IGNORE_THIS", "keep2"]}).lazy()

    result = phase.process(lf).collect()

    assert len(result) == 2
    assert result["field1"][0] == "keep"
    assert result["field1"][1] == "keep2"
