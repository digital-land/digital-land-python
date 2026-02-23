"""Unit tests for concat transform phase using Polars LazyFrame."""
import polars as pl
import pytest
from digital_land.phase_polars.transform.concat import ConcatPhase


def test_concat_basic():
    """Test basic field concatenation."""
    # Create test data
    data = {
        "field1": ["a", "b", "c"],
        "field2": ["x", "y", "z"],
        "field3": ["1", "2", "3"]
    }
    lf = pl.LazyFrame(data)
    
    # Configure concat to combine field1 and field2
    concats = {
        "combined": {
            "fields": ["field1", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    # Apply concat phase
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Verify results
    assert "combined" in result.columns
    assert result["combined"][0] == "a-x"
    assert result["combined"][1] == "b-y"
    assert result["combined"][2] == "c-z"


def test_concat_with_prepend_append():
    """Test concatenation with prepend and append strings."""
    data = {
        "prefix": ["title", "title", "title"],
        "reference": ["123", "456", "789"]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "full_ref": {
            "fields": ["prefix", "reference"],
            "separator": ":",
            "prepend": "[",
            "append": "]"
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    assert result["full_ref"][0] == "[title:123]"
    assert result["full_ref"][1] == "[title:456]"
    assert result["full_ref"][2] == "[title:789]"


def test_concat_with_empty_fields():
    """Test concatenation filtering out empty strings."""
    data = {
        "field1": ["a", "", "c"],
        "field2": ["x", "y", ""],
        "field3": ["1", "2", "3"]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "combined": {
            "fields": ["field1", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Empty strings should be filtered out
    assert result["combined"][0] == "a-x"  # Both fields present
    assert result["combined"][1] == "y"    # Only field2 present
    assert result["combined"][2] == "c"    # Only field1 present


def test_concat_with_null_values():
    """Test concatenation filtering out null values."""
    data = {
        "field1": ["a", None, "c"],
        "field2": ["x", "y", None],
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "combined": {
            "fields": ["field1", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Null values should be filtered out
    assert result["combined"][0] == "a-x"  # Both fields present
    assert result["combined"][1] == "y"    # Only field2 present
    assert result["combined"][2] == "c"    # Only field1 present


def test_concat_multiple_fields():
    """Test concatenation with more than two fields."""
    data = {
        "part1": ["a", "b", "c"],
        "part2": ["x", "y", "z"],
        "part3": ["1", "2", "3"],
        "part4": ["m", "n", "o"]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "full": {
            "fields": ["part1", "part2", "part3", "part4"],
            "separator": ".",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    assert result["full"][0] == "a.x.1.m"
    assert result["full"][1] == "b.y.2.n"
    assert result["full"][2] == "c.z.3.o"


def test_concat_no_config():
    """Test that phase returns unchanged LazyFrame if no concats configured."""
    data = {
        "field1": ["a", "b", "c"],
        "field2": ["x", "y", "z"]
    }
    lf = pl.LazyFrame(data)
    
    # Empty concat config
    phase = ConcatPhase(concats={})
    result = phase.process(lf).collect()
    
    # Should have original columns only
    assert set(result.columns) == {"field1", "field2"}


def test_concat_existing_field():
    """Test concatenation when target field already exists."""
    data = {
        "field1": ["a", "b", "c"],
        "field2": ["x", "y", "z"],
        "combined": ["old", "old", "old"]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "combined": {
            "fields": ["field1", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Should include existing field value first
    assert result["combined"][0] == "old-a-x"
    assert result["combined"][1] == "old-b-y"
    assert result["combined"][2] == "old-c-z"


def test_concat_missing_source_field():
    """Test concatenation when source field doesn't exist in data."""
    data = {
        "field1": ["a", "b", "c"],
        "field2": ["x", "y", "z"]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "combined": {
            "fields": ["field1", "field_missing", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Should concatenate only existing fields
    assert result["combined"][0] == "a-x"
    assert result["combined"][1] == "b-y"
    assert result["combined"][2] == "c-z"


def test_concat_whitespace_only():
    """Test concatenation filtering out whitespace-only strings."""
    data = {
        "field1": ["a", "  ", "c"],
        "field2": ["x", "y", "  "]
    }
    lf = pl.LazyFrame(data)
    
    concats = {
        "combined": {
            "fields": ["field1", "field2"],
            "separator": "-",
            "prepend": "",
            "append": ""
        }
    }
    
    phase = ConcatPhase(concats=concats)
    result = phase.process(lf).collect()
    
    # Whitespace-only strings should be filtered out
    assert result["combined"][0] == "a-x"
    assert result["combined"][1] == "y"
    assert result["combined"][2] == "c"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
