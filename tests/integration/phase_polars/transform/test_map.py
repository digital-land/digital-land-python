#!/usr/bin/env python3
import polars as pl
from digital_land.phase_polars.transform.filter import FilterPhase
from digital_land.phase_polars.transform.map import MapPhase


def test_filter_to_map_integration():
    """Test that Filter output can be passed to Map phase."""
    # Create test data
    lf = pl.LazyFrame({
        "Organisation_Entity": ["1", "2", "3"],
        "Site_Reference": ["A", "B", "C"],
        "Site_Prefix": ["title-1", "title-2", "other-3"]
    })
    
    # Apply filter
    filter_phase = FilterPhase(filters={"Site_Prefix": "^title"})
    lf_filtered = filter_phase.process(lf)
    
    # Apply map
    fieldnames = ["organisation-entity", "reference", "prefix"]
    column_map = {
        "organisation-entity": "organisation-entity",
        "site-reference": "reference",
        "site-prefix": "prefix"
    }
    map_phase = MapPhase(fieldnames=fieldnames, columns=column_map)
    lf_mapped = map_phase.process(lf_filtered)
    
    # Collect and verify
    result = lf_mapped.collect()
    
    assert len(result) == 2
    assert set(result.columns) == {"organisation-entity", "reference", "prefix"}
    assert result["prefix"].to_list() == ["title-1", "title-2"]


def test_map_with_multiple_transformations():
    """Test Map phase with column renaming and dropping."""
    lf = pl.LazyFrame({
        "col_one": [1, 2],
        "col_two": [3, 4],
        "col_ignore": [5, 6]
    })
    
    fieldnames = ["field-one", "field-two"]
    column_map = {
        "col-one": "field-one",
        "col-two": "field-two",
        "col-ignore": "IGNORE"
    }
    
    map_phase = MapPhase(fieldnames=fieldnames, columns=column_map)
    result = map_phase.process(lf).collect()
    
    assert set(result.columns) == {"field-one", "field-two"}
    assert result.to_dicts() == [
        {"field-one": 1, "field-two": 3},
        {"field-one": 2, "field-two": 4}
    ]


if __name__ == "__main__":
    test_filter_to_map_integration()
    test_map_with_multiple_transformations()
    print("All integration tests passed!")
