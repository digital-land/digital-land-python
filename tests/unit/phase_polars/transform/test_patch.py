#!/usr/bin/env python3
import polars as pl
from digital_land.phase_polars.transform.patch import PatchPhase


def test_patch_regex():
    patches = {
        "grade": {
            "^1$": "I",
            "^2$": "II",
            "^2\\*$": "II*",
            "^2 Star$": "II*",
            "^3$": "III",
        }
    }
    
    lf = pl.LazyFrame({
        "grade": ["II", "II*", "2", "2*", "2 Star", "1", "3"]
    })
    
    p = PatchPhase(patches=patches)
    result = p.process(lf).collect()
    
    expected = ["II", "II*", "II", "II*", "II*", "I", "III"]
    assert result["grade"].to_list() == expected


def test_patch_url_with_special_chars():
    patches = {
        "OrganisationURI": {
            "https://example.com/search?query=data&filter=name%20contains%20test": "patch_organisation",
        }
    }
    
    lf = pl.LazyFrame({
        "OrganisationURI": [
            "https://example.com/search?query=data&filter=name%20contains%20test",
            "https://other.com"
        ]
    })
    
    p = PatchPhase(patches=patches)
    result = p.process(lf).collect()
    
    assert result["OrganisationURI"].to_list() == ["patch_organisation", "https://other.com"]


def test_patch_no_change():
    patches = {
        "field": {
            "^old$": "new"
        }
    }
    
    lf = pl.LazyFrame({"field": ["unchanged", "other"]})
    
    p = PatchPhase(patches=patches)
    result = p.process(lf).collect()
    
    assert result["field"].to_list() == ["unchanged", "other"]


def test_patch_empty_patches():
    lf = pl.LazyFrame({"field": ["value1", "value2"]})
    
    p = PatchPhase(patches={})
    result = p.process(lf).collect()
    
    assert result["field"].to_list() == ["value1", "value2"]


def test_patch_global_pattern():
    patches = {
        "": {
            "^test$": "replaced"
        }
    }
    
    lf = pl.LazyFrame({
        "field1": ["test", "other"],
        "field2": ["test", "value"]
    })
    
    p = PatchPhase(patches=patches)
    result = p.process(lf).collect()
    
    assert result["field1"].to_list() == ["replaced", "other"]
    assert result["field2"].to_list() == ["replaced", "value"]


def test_patch_multiple_fields():
    patches = {
        "status": {
            "^pending$": "in-progress"
        },
        "type": {
            "^full$": "full planning permission"
        }
    }
    
    lf = pl.LazyFrame({
        "status": ["pending", "approved"],
        "type": ["full", "outline"]
    })
    
    p = PatchPhase(patches=patches)
    result = p.process(lf).collect()
    
    assert result["status"].to_list() == ["in-progress", "approved"]
    assert result["type"].to_list() == ["full planning permission", "outline"]
