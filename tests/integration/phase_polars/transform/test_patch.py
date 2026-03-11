#!/usr/bin/env python3
import polars as pl
from digital_land.phase_polars.transform.map import MapPhase
from digital_land.phase_polars.transform.patch import PatchPhase


def test_map_to_patch_integration():
    """Test that Map output can be passed to Patch phase."""
    # Create test data
    lf = pl.LazyFrame(
        {"Site_Status": ["pending", "approved"], "Permission_Type": ["full", "outline"]}
    )

    # Apply map
    fieldnames = ["status", "permission-type"]
    column_map = {"site-status": "status", "permission-type": "permission-type"}
    map_phase = MapPhase(fieldnames=fieldnames, columns=column_map)
    lf_mapped = map_phase.process(lf)

    # Apply patch
    patches = {
        "status": {"^pending$": "in-progress"},
        "permission-type": {"^full$": "full planning permission"},
    }
    patch_phase = PatchPhase(patches=patches)
    lf_patched = patch_phase.process(lf_mapped)

    # Collect and verify
    result = lf_patched.collect()

    assert result["status"].to_list() == ["in-progress", "approved"]
    assert result["permission-type"].to_list() == [
        "full planning permission",
        "outline",
    ]


if __name__ == "__main__":
    test_map_to_patch_integration()
    print("All integration tests passed!")
