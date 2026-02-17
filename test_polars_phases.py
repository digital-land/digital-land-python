#!/usr/bin/env python3
"""
Test script for Polars-based pipeline phases.

Creates a simple CSV, runs each polars phase individually and in chain,
and verifies the output matches expectations.
"""

import os
import sys
import tempfile
import logging

logging.basicConfig(level=logging.DEBUG, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Create test CSV ──────────────────────────────────────────────────────────
TEST_CSV_CONTENT = """\
reference,name,geometry,documentation-url,start-date,organisation,entry-date
ref-001,Test Area One,MULTIPOLYGON(((-0.1 51.5,-0.1 51.6,-0.2 51.6,-0.2 51.5,-0.1 51.5))),https://example.com/doc1,2024-01-15,local-authority-eng:example,2024-01-15
ref-002,Test Area Two,MULTIPOLYGON(((-0.3 51.5,-0.3 51.6,-0.4 51.6,-0.4 51.5,-0.3 51.5))),https://example.com/doc2,2024-02-20,local-authority-eng:example,2024-02-20
ref-003,"  Test Area Three  ",MULTIPOLYGON(((-0.5 51.5,-0.5 51.6,-0.6 51.6,-0.6 51.5,-0.5 51.5))),https://example.com/doc3,2024-03-10,local-authority-eng:example,2024-03-10
"""

tmp_dir = tempfile.mkdtemp(prefix="polars_phases_test_")
input_csv = os.path.join(tmp_dir, "test_input.csv")
output_csv = os.path.join(tmp_dir, "test_output.csv")

with open(input_csv, "w") as f:
    f.write(TEST_CSV_CONTENT)

print(f"Test data written to: {input_csv}")
print(f"Output will go to: {output_csv}")

# ── Import polars phases ────────────────────────────────────────────────────
import polars as pl

from digital_land.phase_polars import (
    run_polars_pipeline,
    ConvertPhase,
    NormalisePhase,
    ConcatFieldPhase,
    FilterPhase,
    MapPhase,
    PatchPhase,
    HarmonisePhase,
    DefaultPhase,
    MigratePhase,
    OrganisationPhase,
    FieldPrunePhase,
    EntityPrunePhase,
    FactPrunePhase,
    EntityReferencePhase,
    EntityPrefixPhase,
    EntityLookupPhase,
    FactLookupPhase,
    SavePhase,
    PivotPhase,
    FactCombinePhase,
    FactorPhase,
    PriorityPhase,
    DumpPhase,
    LoadPhase,
)
from digital_land.log import DatasetResourceLog, ConvertedResourceLog, ColumnFieldLog, IssueLog

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  PASS: {name}")
        passed += 1
    else:
        print(f"  FAIL: {name} {detail}")
        failed += 1


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: ConvertPhase — loads CSV into DataFrame
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 1: ConvertPhase ──")
dataset_resource_log = DatasetResourceLog()
converted_resource_log = ConvertedResourceLog()
convert_phase = ConvertPhase(
    path=input_csv,
    dataset_resource_log=dataset_resource_log,
    converted_resource_log=converted_resource_log,
)
df = convert_phase.process()
check("returns DataFrame", isinstance(df, pl.DataFrame))
check("has 3 rows", df.height == 3, f"got {df.height}")
check("has __resource column", "__resource" in df.columns)
check("has __line_number column", "__line_number" in df.columns)
check("has reference column", "reference" in df.columns)
print(f"  Columns: {[c for c in df.columns if not c.startswith('__')]}")
print(f"  Shape: {df.shape}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2: NormalisePhase — strips whitespace
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 2: NormalisePhase ──")
normalise_phase = NormalisePhase(skip_patterns=[])
df2 = normalise_phase.process(df)
check("preserves row count", df2.height == 3)
# Check that whitespace was stripped from "  Test Area Three  "
names = df2["name"].to_list()
check("whitespace stripped", "Test Area Three" in names, f"got {names}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3: MapPhase — renames columns
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 3: MapPhase ──")
fieldnames = [
    "reference", "name", "geometry", "documentation-url",
    "start-date", "organisation", "entry-date", "point",
    "entity", "prefix", "end-date",
]
column_field_log = ColumnFieldLog()
map_phase = MapPhase(fieldnames=fieldnames, columns={}, log=column_field_log)
df3 = map_phase.process(df2)
check("preserves row count", df3.height == 3)
check("has reference column", "reference" in df3.columns)
data_cols = [c for c in df3.columns if not c.startswith("__")]
print(f"  Mapped columns: {data_cols}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: PatchPhase — applies patches
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 4: PatchPhase ──")
issue_log = IssueLog(dataset="test-dataset", resource="test-resource")
patch_phase = PatchPhase(issues=issue_log, patches={})
df4 = patch_phase.process(df3)
check("no patches, same rows", df4.height == 3)

# Test with actual patches
patch_with_data = PatchPhase(
    issues=issue_log,
    patches={"name": {"Test Area One": "Patched Area One"}},
)
df4b = patch_with_data.process(df3)
names_patched = df4b["name"].to_list()
check("patch applied", "Patched Area One" in names_patched, f"got {names_patched}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5: DefaultPhase — applies defaults
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 5: DefaultPhase ──")
default_phase = DefaultPhase(
    issues=issue_log,
    default_values={"end-date": ""},
)
# Add an empty end-date column
df5_in = df4.with_columns(pl.lit("").alias("end-date"))
df5 = default_phase.process(df5_in)
check("preserves rows", df5.height == 3)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: FilterPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 6: FilterPhase ──")
filter_phase = FilterPhase(filters={})
df6 = filter_phase.process(df5)
check("no filter, same rows", df6.height == 3)

filter_with_data = FilterPhase(filters={"reference": "ref-001"})
df6b = filter_with_data.process(df5)
check("filter applied", df6b.height == 1, f"got {df6b.height}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7: MigratePhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 7: MigratePhase ──")
migrate_phase = MigratePhase(
    fields=["reference", "name", "geometry", "documentation-url",
            "start-date", "organisation", "entry-date", "end-date"],
    migrations={},
)
df7 = migrate_phase.process(df6)
check("preserves rows", df7.height == 3)
check("has reference", "reference" in df7.columns)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8: EntityReferencePhase + EntityPrefixPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 8: EntityReferencePhase + EntityPrefixPhase ──")
ref_phase = EntityReferencePhase(dataset="test-dataset", prefix="test-dataset", issues=issue_log)
df8 = ref_phase.process(df7)
check("has prefix", "prefix" in df8.columns)
check("has reference", "reference" in df8.columns)
prefixes = df8["prefix"].to_list()
check("prefix set", all(p == "test-dataset" for p in prefixes), f"got {prefixes}")

prefix_phase = EntityPrefixPhase(dataset="test-dataset")
df8b = prefix_phase.process(df8)
check("prefix still set", all(p == "test-dataset" for p in df8b["prefix"].to_list()))

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 9: FieldPrunePhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 9: FieldPrunePhase ──")
prune_phase = FieldPrunePhase(fields=["reference", "name", "geometry", "organisation"])
df9 = prune_phase.process(df8b)
data_cols9 = [c for c in df9.columns if not c.startswith("__")]
check("pruned to expected fields", len(data_cols9) <= 8, f"got {data_cols9}")
check("has reference", "reference" in df9.columns)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 10: EntityLookupPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 10: EntityLookupPhase ──")
from digital_land.phase_polars.lookup import key as lookup_key
lookups = {
    lookup_key(prefix="test-dataset", reference="ref-001"): "1000001",
    lookup_key(prefix="test-dataset", reference="ref-002"): "1000002",
    lookup_key(prefix="test-dataset", reference="ref-003"): "1000003",
}
lookup_phase = EntityLookupPhase(
    lookups=lookups,
    redirect_lookups={},
    issue_log=issue_log,
    entity_range=[1000000, 2000000],
)
df10 = lookup_phase.process(df9)
check("has entity column", "entity" in df10.columns)
entities = df10["entity"].to_list()
check("entities assigned", "1000001" in entities, f"got {entities}")
check("all entities assigned", all(e for e in entities), f"got {entities}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 11: EntityPrunePhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 11: EntityPrunePhase ──")
dataset_resource_log2 = DatasetResourceLog(dataset="test-dataset", resource="test-resource")
entity_prune = EntityPrunePhase(dataset_resource_log=dataset_resource_log2)
df11 = entity_prune.process(df10)
check("all rows kept (all have entities)", df11.height == 3)
check("entry count logged", dataset_resource_log2.entry_count == 3)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 12: PriorityPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 12: PriorityPhase ──")
priority_phase = PriorityPhase(config=None, providers=[])
df12 = priority_phase.process(df11)
check("has __priority", "__priority" in df12.columns)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 13: PivotPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 13: PivotPhase ──")
pivot_phase = PivotPhase()
df13 = pivot_phase.process(df12)
check("pivoted to facts", df13.height > 3, f"got {df13.height} rows (should be > 3)")
check("has fact column", "fact" in df13.columns)
check("has field column", "field" in df13.columns)
check("has value column", "value" in df13.columns)
print(f"  Pivoted to {df13.height} fact rows from 3 entity rows")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 14: FactorPhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 14: FactorPhase ──")
factor_phase = FactorPhase()
df14 = factor_phase.process(df13)
facts = df14["fact"].to_list()
non_empty_facts = [f for f in facts if f]
check("fact hashes generated", len(non_empty_facts) > 0, f"got {len(non_empty_facts)}")
check("fact is sha256 hex", len(non_empty_facts[0]) == 64 if non_empty_facts else False)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 15: FactPrunePhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 15: FactPrunePhase ──")
fact_prune = FactPrunePhase()
df15 = fact_prune.process(df14)
check("facts pruned (empty values removed)", df15.height <= df14.height)
print(f"  Before: {df14.height} → After: {df15.height}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 16: SavePhase
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 16: SavePhase ──")
save_phase = SavePhase(path=output_csv, fieldnames=["entity", "fact", "field", "value"])
df16 = save_phase.process(df15)
check("CSV file created", os.path.exists(output_csv))
if os.path.exists(output_csv):
    import csv
    with open(output_csv) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    check("CSV has rows", len(rows) > 0, f"got {len(rows)}")
    check("CSV has entity column", "entity" in rows[0])
    print(f"  Saved {len(rows)} rows to {output_csv}")

# ═══════════════════════════════════════════════════════════════════════════════
# TEST 17: run_polars_pipeline (chained execution)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Test 17: run_polars_pipeline (chained) ──")
chain_output = os.path.join(tmp_dir, "chain_output.csv")
result_df = run_polars_pipeline(
    ConvertPhase(path=input_csv),
    NormalisePhase(),
    MapPhase(fieldnames=fieldnames, columns={}),
    FilterPhase(filters={}),
    SavePhase(path=chain_output, enabled=True),
)
check("chain returns DataFrame", isinstance(result_df, pl.DataFrame))
check("chain output file exists", os.path.exists(chain_output))
if os.path.exists(chain_output):
    result_check = pl.read_csv(chain_output)
    check("chain output has 3 rows", result_check.height == 3, f"got {result_check.height}")

# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print(f"RESULTS: {passed} passed, {failed} failed out of {passed + failed} checks")
print("=" * 70)

if failed > 0:
    print("\nSome tests FAILED!")
    sys.exit(1)
else:
    print("\nAll tests PASSED!")
    sys.exit(0)
