"""Unit tests for filter transform phase using Polars LazyFrame."""
import polars as pl
import pytest
from digital_land.phase_polars.transform.filter import FilterPhase


def test_filter_basic_match():
    """Test basic field filtering with pattern matching."""
    # Create test data
    data = {
        "reference": ["1", "2", "3"],
        "name": ["One", "Two", "Three"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for names starting with "T"
    filters = {"name": "^T"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should only include rows where name starts with "T"
    assert len(result) == 2
    assert result["name"][0] == "Two"
    assert result["name"][1] == "Three"
    assert result["reference"][0] == "2"
    assert result["reference"][1] == "3"


def test_filter_negative_pattern():
    """Test filtering with negative lookahead pattern."""
    data = {
        "reference": ["1", "2", "3"],
        "somefield": ["Group", "Individual", "Zone"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter to exclude rows starting with "Individual"
    filters = {"somefield": "^(?!Individual).*"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should include only "Group" and "Zone"
    assert len(result) == 2
    assert result["somefield"][0] == "Group"
    assert result["somefield"][1] == "Zone"
    assert result["reference"][0] == "1"
    assert result["reference"][1] == "3"


def test_filter_multiple_fields():
    """Test filtering with multiple field patterns."""
    data = {
        "reference": ["1", "2", "3", "4"],
        "name": ["Alice", "Bob", "Charlie", "David"],
        "status": ["active", "inactive", "active", "active"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for names starting with "A" or "C" AND status is "active"
    filters = {
        "name": "^[AC]",
        "status": "^active$"
    }
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should include only Alice and Charlie (both match name pattern and have active status)
    assert len(result) == 2
    assert result["name"][0] == "Alice"
    assert result["name"][1] == "Charlie"
    assert result["reference"][0] == "1"
    assert result["reference"][1] == "3"


def test_filter_no_matches():
    """Test filtering when no rows match the pattern."""
    data = {
        "reference": ["1", "2", "3"],
        "name": ["One", "Two", "Three"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for names starting with "Z" (none match)
    filters = {"name": "^Z"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should return empty dataframe
    assert len(result) == 0


def test_filter_all_match():
    """Test filtering when all rows match the pattern."""
    data = {
        "reference": ["1", "2", "3"],
        "prefix": ["title-boundary", "title-document", "title-record"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for prefix starting with "title"
    filters = {"prefix": "^title"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should return all rows
    assert len(result) == 3
    assert result["reference"][0] == "1"
    assert result["reference"][1] == "2"
    assert result["reference"][2] == "3"


def test_filter_no_config():
    """Test that phase returns unchanged LazyFrame if no filters configured."""
    data = {
        "reference": ["1", "2", "3"],
        "name": ["One", "Two", "Three"]
    }
    lf = pl.LazyFrame(data)
    
    # Empty filter config
    phase = FilterPhase(filters={})
    result = phase.process(lf).collect()
    
    # Should return all rows unchanged
    assert len(result) == 3
    assert list(result["reference"]) == ["1", "2", "3"]


def test_filter_missing_field():
    """Test filtering when filter field doesn't exist in data."""
    data = {
        "reference": ["1", "2", "3"],
        "name": ["One", "Two", "Three"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter on a field that doesn't exist
    filters = {"missing_field": "^test"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should return all rows since filter field doesn't exist
    assert len(result) == 3


def test_filter_with_null_values():
    """Test filtering behavior with null values."""
    data = {
        "reference": ["1", "2", "3", "4"],
        "name": ["Alice", None, "Charlie", ""]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for names starting with "A" or "C"
    filters = {"name": "^[AC]"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should only include Alice and Charlie (null and empty string don't match)
    assert len(result) == 2
    assert result["name"][0] == "Alice"
    assert result["name"][1] == "Charlie"


def test_filter_case_sensitive():
    """Test that filtering is case-sensitive by default."""
    data = {
        "reference": ["1", "2", "3"],
        "name": ["apple", "Apple", "APPLE"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for lowercase "apple"
    filters = {"name": "^apple$"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should only match exact lowercase "apple"
    assert len(result) == 1
    assert result["name"][0] == "apple"


def test_filter_with_special_characters():
    """Test filtering with special regex characters."""
    data = {
        "reference": ["1", "2", "3"],
        "email": ["user@example.com", "admin@test.org", "info@sample.net"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for emails ending with ".com"
    filters = {"email": r"\.com$"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should only match .com email
    assert len(result) == 1
    assert result["email"][0] == "user@example.com"


def test_filter_partial_match():
    """Test filtering with patterns that match anywhere in the string."""
    data = {
        "reference": ["1", "2", "3"],
        "description": ["This is a test", "Another example", "Testing again"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for descriptions containing "test" (case-insensitive would need flag)
    filters = {"description": "test"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should match rows containing "test"
    assert len(result) == 1
    assert result["description"][0] == "This is a test"


def test_filter_empty_string():
    """Test filtering behavior with empty strings."""
    data = {
        "reference": ["1", "2", "3", "4"],
        "name": ["Alice", "", "Charlie", "David"]
    }
    lf = pl.LazyFrame(data)
    
    # Filter for non-empty names
    filters = {"name": ".+"}
    
    phase = FilterPhase(filters=filters)
    result = phase.process(lf).collect()
    
    # Should exclude the empty string
    assert len(result) == 3
    assert result["name"][0] == "Alice"
    assert result["name"][1] == "Charlie"
    assert result["name"][2] == "David"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
