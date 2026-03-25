# phase_polars

Polars-based implementations of the digital-land data pipeline phases, rewriting the legacy stream-based phases in `digital_land/phase/` using [Polars](https://pola.rs/) `LazyFrame`s for improved performance and throughput.

---

## Contents

- [Transform Phases](#transform-phases)
- [Load Phases](#load-phases)
- [Pipeline Composition and Execution Order](#pipeline-composition-and-execution-order)
- [Usage Example](#usage-example)
- [Comparison with the Legacy Phase Pipeline](#comparison-with-the-legacy-phase-pipeline)
- [Issue Logging](#issue-logging)
- [Deployment Status and Infrastructure Constraints](#deployment-status-and-infrastructure-constraints)
- [Developer Guide](#developer-guide)

---

## Transform Phases

The `transform/` folder contains the core data transformation phases.

| File | Status | Description |
|------|--------|-------------|
| `concat.py` | Implemented | Concatenates multiple fields into one using a configurable separator, prefix, and suffix. |
| `convert.py` | Implemented | Detects file encoding and converts various input formats (CSV, JSON, GeoJSON, ZIP, SQLite, shapefiles) into a normalised `LazyFrame`. |
| `entity_lookup.py` | Stub | Not yet implemented. |
| `entity_reference.py` | Stub | Not yet implemented. |
| `fact_hash.py` | Stub | Not yet implemented. |
| `field_prune.py` | Stub | Not yet implemented. |
| `filter.py` | Implemented | Filters rows by matching field values against regex patterns. |
| `flatten.py` | Stub | Not yet implemented. |
| `harmonise.py` | Implemented | Harmonises field values (dates, URIs, references, geometries) to canonical formats. |
| `map.py` | Implemented | Renames raw column headers to canonical field names. |
| `migrate.py` | Stub | Not yet implemented. |
| `normalise.py` | Implemented | Strips whitespace and null-like values from all fields. |
| `parse.py` | Implemented | Adds a 1-based `entry-number` row index column. |
| `patch.py` | Implemented | Applies regex-based find-and-replace patches to specific fields (or all fields). |
| `pivot.py` | Stub | Not yet implemented. |
| `priority.py` | Stub | Not yet implemented. |
| `resolve_organisation.py` | Stub | Not yet implemented. |
| `set_default.py` | Stub | Not yet implemented. |
| `validate.py` | Stub | Not yet implemented. |

## Load Phases

The `load/` folder contains phases for persisting data. Both are currently stubs.

| File | Status |
|------|--------|
| `save_file.py` | Stub — not yet implemented. |
| `save_database.py` | Stub — not yet implemented. |

---

## Pipeline Composition and Execution Order

The Polars phases occupy a middle segment of the full pipeline managed by `Pipeline.transform()` in [digital_land/pipeline/main.py](../pipeline/main.py). A bridge converts the legacy convert stream to a `LazyFrame` before the Polars phases begin, and a second bridge emits a stream afterwards for the remaining legacy phases.

| # | Phase | |
|---|-------|-|
| 1 | `ConvertPhase` | Legacy stream |
| — | *stream → LazyFrame* | `StreamToPolarsConverter.from_stream()` |
| 2 | `NormalisePhase` | **phase_polars** |
| 3 | `ParsePhase` | **phase_polars** |
| 4 | `ConcatPhase` | **phase_polars** |
| 5 | `FilterPhase` *(pre-map)* | **phase_polars** |
| 6 | `MapPhase` | **phase_polars** |
| 7 | `FilterPhase` *(post-map)* | **phase_polars** |
| 8 | `PatchPhase` | **phase_polars** |
| 9 | `HarmonisePhase` | **phase_polars** |
| — | *LazyFrame → stream* | `polars_to_stream()` |
| 10–20 | `DefaultPhase` → `SavePhase` (final) | Legacy stream |

Phases 2–9 are the current scope of `phase_polars`. Everything else remains on the legacy stream path.

---

## Usage Example

Full working example is in [tests/acceptance/polars/test_harmonise_comparison.py](../../tests/acceptance/polars/test_harmonise_comparison.py). The pattern:

```python
from digital_land.phase.convert import ConvertPhase
from digital_land.pipeline.main import StreamToPolarsConverter
from digital_land.phase_polars.transform.normalise import NormalisePhase
from digital_land.phase_polars.transform.parse import ParsePhase
from digital_land.phase_polars.transform.concat import ConcatPhase
from digital_land.phase_polars.transform.filter import FilterPhase
from digital_land.phase_polars.transform.map import MapPhase
from digital_land.phase_polars.transform.patch import PatchPhase
from digital_land.phase_polars.transform.harmonise import HarmonisePhase

stream = ConvertPhase(path="input.csv").process(None)
lf = StreamToPolarsConverter.from_stream(stream)

lf = NormalisePhase(skip_patterns=[]).process(lf)
lf = ParsePhase().process(lf)
lf = ConcatPhase(concats={"address": {"fields": ["street", "town"], "separator": ", "}}).process(lf)
lf = FilterPhase(filters={"organisation": "^active"}).process(lf)
lf = MapPhase(fieldnames=fieldnames, columns=column_map).process(lf)
lf = FilterPhase(filters=endpoint_filters).process(lf)
lf = PatchPhase(patches={"": {"N/A": ""}}).process(lf)  # "" applies to all fields
lf = HarmonisePhase(
    field_datatype_map={"start-date": "date", "geometry": "geometry"},
    dataset="conservation-area",
    valid_category_values={"category": ["A", "B"]},
).process(lf)

df = lf.collect()  # triggers Rust execution
```

**Constructor reference:**

| Class | Key arguments |
|---|---|
| `NormalisePhase` | `skip_patterns: list[str]` |
| `ParsePhase` | *(none)* |
| `ConcatPhase` | `concats: dict` — `{output_field: {"fields": [...], "separator", "prepend", "append"}}` |
| `FilterPhase` | `filters: dict` — `{field_name: regex_pattern}` |
| `MapPhase` | `fieldnames: list[str]`, `columns: dict` — `{normalised_column: canonical_field}` |
| `PatchPhase` | `patches: dict` — `{field_name: {find: replacement}}`; `""` key matches all fields |
| `HarmonisePhase` | `field_datatype_map: dict`, `dataset: str`, `valid_category_values: dict` |

---

## Comparison with the Legacy Phase Pipeline

The legacy phases in `digital_land/phase/` are a Python generator chain — each phase receives and yields one `block` dict at a time, carrying `row`, `resource`, `line-number`, and `entry-number`. The entire pipeline is single-threaded Python iteration.

The Polars phases have no base class. Each accepts a `pl.LazyFrame`, builds an expression plan, and returns a new `pl.LazyFrame`. No data is processed until `.collect()` is called, at which point Rust executes all transformations in parallel.

| Characteristic | Legacy | Polars |
|---|---|---|
| Execution | Python generator, one row at a time | Rust, all rows at once |
| Memory | O(1) — one row in flight | O(n) — full dataset on `.collect()` |
| Python per row | Yes | No — Python only builds the plan |
| Parallelism | None | Automatic across columns and row-chunks |
| Per-row logging | Natural | Not possible during execution (see below) |

---

## Issue Logging

### Why the legacy approach cannot be directly ported

The legacy `IssueLog` works by stamping row identity onto a shared mutable object before each field is processed (`self.issues.line_number = block["line-number"]`). This is impossible in a LazyFrame pipeline because Polars executes in Rust with no Python callback per row. The current `harmonise.py` uses a `_NoOpIssues` adapter that silently discards all log calls — issue logging is entirely absent from the Polars path today.

Using `map_elements()` to force per-cell Python callbacks is not a viable workaround: it disables predicate pushdown, parallelism, and type inference, negating the performance benefit.

### How to add it back

Three approaches, in order of preference:

**Option A — Post-collect diff (recommended).** Collect relevant columns before and after the phase, then compute changed cells with vectorised comparisons. Transformation stays fully in Rust; Python only reads the diff. Requires `"entry-number"` to be present (added by `parse.py`) for row attribution.

**Option B — Sentinel columns.** Add a temporary `_issue_{field}` column during the transformation expression that captures the original value when a change occurs. After a single `.collect()`, filter on non-null sentinel columns to build the issue rows, then drop the sentinels. No double-collect needed.

**Option C — Column-level list comprehension (fallback).** `harmonise.py` already uses this for datatype normalisers that cannot be expressed as Polars expressions. Collect a column as a Python list, run a comprehension that writes to a side-channel buffer, convert the buffer to a `pl.DataFrame`. One Python call per cell but batched at the column level.

Issue output should be a `pl.DataFrame` with columns: `entry-number`, `field`, `value`, `issue-type` — mirroring the `IssueLog` schema in `digital_land/log.py`.

---

## Deployment Status and Infrastructure Constraints

The Polars-enhanced phases were successfully deployed to the development environment for testing. However, the current infrastructure is unsuitable for vectorised processing in its present form — a risk noted in the initial design.

Three blockers have been identified:

- **ECS instances are underpowered.** The current compute profile suits lightweight tasks, not sustained analytical workloads. On under-resourced instances the working set can exceed available memory, eliminating the performance advantage.
- **CPU credit throttling.** Burstable instance families (e.g. `t3`/`t4g`) drop sharply in available CPU once credits are exhausted, making benchmark results unreliable mid-run.
- **Multithreading is restricted.** Task-level CPU limits or container runtime settings prevent Polars from using available cores, masking the parallel execution gains seen in controlled tests.

**Recommended changes:** move pipeline tasks to compute-optimised instance types (`c6i`/`c7g`); avoid burstable families or enable unlimited burst; ensure ECS task CPU reservation matches container vCPUs and that `POLARS_MAX_THREADS` is not capped. For very large datasets, consider `collect(streaming=True)` to reduce peak memory pressure.

Do not treat ECS benchmark results as meaningful until these constraints are addressed. Local benchmarks on hardware with at least 4 vCPUs are the only currently reliable measurement.

---

## Developer Guide

### Writing a new phase

Each stub is an empty module. Add a single class with `process(self, lf: pl.LazyFrame) -> pl.LazyFrame`. Read the equivalent legacy phase in `digital_land/phase/` for expected semantics, then translate:
- `yield` to conditionally drop rows → `.filter()`
- Mutate a field value → `.with_columns(pl.when(...).then(...).otherwise(pl.col(field)))`

The implemented phases (`normalise.py`, `patch.py`, `filter.py`, `harmonise.py`) are the best structural reference.

Before writing any logic: a `LazyFrame` is a query plan. Calling `.with_columns()` or `.filter()` does not touch data — execution happens in Rust when `.collect()` is called. If you find yourself wanting to inspect individual rows, restructure as a Polars expression. The [Polars lazy evaluation guide](https://docs.pola.rs/user-guide/lazy/) is worth reading first.

### Issue logging

Issue logging must be added explicitly — it will not happen automatically. See the [Issue Logging](#issue-logging) section for the three approaches. Ensure `"entry-number"` is present (added by `parse.py`) before any phase that needs row attribution.

Avoid `map_elements()` except where genuinely unavoidable. Treat the existing usages in `harmonise.py` as known technical debt, not a pattern to copy.

### Schema compatibility

The goal is output equivalent to the legacy pipeline. Key gotchas when implementing a phase:
- **Null vs empty string** — Polars uses `null`; the legacy pipeline uses empty string as "no value". Coerce at phase boundaries.
- **Column order** — use `.select()` to enforce a stable output schema if downstream consumers depend on ordering.
- **Load phases** — `save_file.py` and `save_database.py` are empty. Before implementing, confirm output paths and serialisation formats match `save.py`/`dump.py` in the legacy path — a mismatch will silently break downstream consumers.

### Testing

Unit-test each phase with a small constructed `LazyFrame`. Cross-validate against the legacy phase using the same CSV fixture — assert outputs match after normalising null representation and column order. Do not run performance benchmarks in CI or on ECS until the infrastructure constraints above are resolved.

---

## Design Principles

- **Modular** — phases can be used independently or composed in sequence.
- **Lazy** — phases operate on `LazyFrame`s; execution is deferred until `.collect()`.
- **Compatible** — outputs must match the legacy stream-based phases in `digital_land/phase/`.

- [Transform Phases](#transform-phases)
- [Load Phases](#load-phases)
- [Pipeline Composition and Execution Order](#pipeline-composition-and-execution-order)
- [Usage Example](#usage-example)
- [Comparison with the Legacy Phase Pipeline](#comparison-with-the-legacy-phase-pipeline)
- [Issue Logging: the Legacy Approach and Why It Cannot be Directly Ported](#issue-logging-the-legacy-approach-and-why-it-cannot-be-directly-ported)
  - [How the legacy pipeline logs issues](#how-the-legacy-pipeline-logs-issues)
  - [Why this pattern breaks in a LazyFrame pipeline](#why-this-pattern-breaks-in-a-lazyframe-pipeline)
  - [Approaches for re-adding issue logging to the Polars path](#approaches-for-re-adding-issue-logging-to-the-polars-path)
- [Deployment Status and Infrastructure Constraints](#deployment-status-and-infrastructure-constraints)
  - [ECS instance sizing](#ecs-instance-sizing)
  - [CPU credit throttling](#cpu-credit-throttling)
  - [Multithreading restrictions](#multithreading-restrictions)
  - [Recommended infrastructure changes](#recommended-infrastructure-changes)
- [Developer Guide: Picking This Up for Further Development](#developer-guide-picking-this-up-for-further-development)
  - [Understand the execution model before writing any phase logic](#understand-the-execution-model-before-writing-any-phase-logic)
  - [Always ensure `"entry-number"` is present before issue logging](#always-ensure-entry-number-is-present-before-issue-logging)
  - [Implementing a stub phase](#implementing-a-stub-phase)
  - [Issue logging must be added explicitly](#issue-logging-must-be-added-explicitly--it-will-not-happen-automatically)
  - [Do not break output schema compatibility with the legacy phases](#do-not-break-output-schema-compatibility-with-the-legacy-phases)
  - [Avoid `map_elements` unless there is no alternative](#avoid-map_elements-unless-there-is-no-alternative)
  - [Load phases are entirely unimplemented](#load-phases-are-entirely-unimplemented)
  - [Testing approach](#testing-approach)
  - [Infrastructure prerequisite before benchmarking](#infrastructure-prerequisite-before-benchmarking)
- [Design Principles](#design-principles)

---

## Transform Phases

The `transform/` folder contains the core data transformation phases. Files are named by function rather than execution order.

| File | Status | Description |
|------|--------|-------------|
| `concat.py` | Implemented | Concatenates multiple fields into one using a configurable separator, prefix, and suffix. |
| `convert.py` | Implemented | Detects file encoding and converts various input formats (CSV, JSON, GeoJSON, ZIP, SQLite, shapefiles) into a normalised `LazyFrame`. |
| `entity_lookup.py` | Stub | Lookup and enrich entity data — not yet implemented. |
| `entity_reference.py` | Stub | Handle entity references — not yet implemented. |
| `fact_hash.py` | Stub | Generate fact hashes for deduplication — not yet implemented. |
| `field_prune.py` | Stub | Remove unnecessary fields — not yet implemented. |
| `filter.py` | Implemented | Filters rows by matching field values against regex patterns defined in pipeline configuration. |
| `flatten.py` | Stub | Flatten nested data structures — not yet implemented. |
| `harmonise.py` | Implemented | Harmonises field values (dates, URIs, references, geometries, etc.) to canonical formats, mirroring legacy stream harmonisation behaviour. |
| `map.py` | Implemented | Renames raw column headers to canonical field names using a column map and normalisation rules. |
| `migrate.py` | Stub | Migrate data structure/format — not yet implemented. |
| `normalise.py` | Implemented | Strips whitespace and null-like values from all fields using patterns from `patch/null.csv`. |
| `parse.py` | Implemented | Adds a 1-based `entry-number` row index column to the `LazyFrame`. |
| `patch.py` | Implemented | Applies regex-based find-and-replace patches to specific fields (or all fields). |
| `pivot.py` | Stub | Pivot data structure — not yet implemented. |
| `priority.py` | Stub | Priority resolution across multiple sources — not yet implemented. |
| `resolve_organisation.py` | Stub | Resolve and enrich organisation references — not yet implemented. |
| `set_default.py` | Stub | Set default values for missing data — not yet implemented. |
| `validate.py` | Stub | Validate data against schema — not yet implemented. |

## Load Phases

The `load/` folder contains phases for persisting data. Both are currently stubs.

| File | Status | Description |
|------|--------|-------------|
| `save_file.py` | Stub | Save data to file storage — not yet implemented. |
| `save_database.py` | Stub | Save data to database — not yet implemented. |

---

## Pipeline Composition and Execution Order

The Polars phases do not run in isolation — they occupy a middle segment of the full pipeline managed by `Pipeline.transform()` in [digital_land/pipeline/main.py](../pipeline/main.py). The legacy `ConvertPhase` always runs first as a stream, and a bridge converts that stream to a `LazyFrame` before the Polars phases begin. After the final Polars phase (`HarmonisePhase`), a second bridge emits a stream and the remaining legacy phases continue as normal.

### Full pipeline sequence

| # | Phase | Path |
|---|-------|------|
| 1 | `ConvertPhase` | Legacy stream (`digital_land/phase/convert.py`) |
| — | *stream → LazyFrame bridge* | `StreamToPolarsConverter.from_stream()` |
| 2 | `NormalisePhase` | `phase_polars/transform/normalise.py` |
| 3 | `ParsePhase` | `phase_polars/transform/parse.py` |
| 4 | `ConcatPhase` | `phase_polars/transform/concat.py` |
| 5 | `FilterPhase` *(pre-map, resource-level)* | `phase_polars/transform/filter.py` |
| 6 | `MapPhase` | `phase_polars/transform/map.py` |
| 7 | `FilterPhase` *(post-map, endpoint-level)* | `phase_polars/transform/filter.py` |
| 8 | `PatchPhase` | `phase_polars/transform/patch.py` |
| 9 | `HarmonisePhase` | `phase_polars/transform/harmonise.py` |
| — | *LazyFrame → stream bridge* | `polars_to_stream()` |
| 10 | `DefaultPhase` | Legacy stream |
| 11 | `MigratePhase` | Legacy stream |
| 12 | `OrganisationPhase` | Legacy stream |
| 13 | `FieldPrunePhase` | Legacy stream |
| 14 | `EntityReferencePhase` | Legacy stream |
| 15 | `EntityLookupPhase` *(optional)* | Legacy stream |
| 16 | `SavePhase` *(harmonised intermediate)* | Legacy stream |
| 17 | `PriorityPhase` | Legacy stream |
| 18 | `PivotPhase` | Legacy stream |
| 19 | `FactorPhase` + fact phases | Legacy stream |
| 20 | `SavePhase` *(final output)* | Legacy stream |

Phases 2–9 are the current scope of `phase_polars`. All phases outside that range remain on the legacy stream path and are not yet ported.

---

## Usage Example

The following illustrates how to instantiate and chain the implemented Polars phases. This mirrors the pattern used in [tests/acceptance/polars/test_harmonise_comparison.py](../../tests/acceptance/polars/test_harmonise_comparison.py) and `_PolarsPhases` in `digital_land/commands.py`.

```python
import polars as pl
from digital_land.phase.convert import ConvertPhase
from digital_land.phase_polars.transform.normalise import NormalisePhase
from digital_land.phase_polars.transform.parse import ParsePhase
from digital_land.phase_polars.transform.concat import ConcatPhase
from digital_land.phase_polars.transform.filter import FilterPhase
from digital_land.phase_polars.transform.map import MapPhase
from digital_land.phase_polars.transform.patch import PatchPhase
from digital_land.phase_polars.transform.harmonise import HarmonisePhase

# Phase 1: legacy convert — produces a stream from the source file
stream = ConvertPhase(path="input.csv").process(None)

# Bridge: convert the stream to a LazyFrame
from digital_land.pipeline.main import StreamToPolarsConverter
lf = StreamToPolarsConverter.from_stream(stream)

# Phases 2–9: Polars transform chain
lf = NormalisePhase(skip_patterns=[]).process(lf)
lf = ParsePhase().process(lf)
lf = ConcatPhase(concats={"address": {"fields": ["street", "town"], "separator": ", "}}).process(lf)
lf = FilterPhase(filters={"organisation": "^(active|relevant)"}).process(lf)
lf = MapPhase(fieldnames=fieldnames, columns=column_map).process(lf)
lf = FilterPhase(filters=endpoint_filters).process(lf)
lf = PatchPhase(patches={"": {"N/A": ""}}).process(lf)  # "" key applies to all fields
lf = HarmonisePhase(
    field_datatype_map={"start-date": "date", "geometry": "geometry"},
    dataset="conservation-area",
    valid_category_values={"category": ["A", "B"]},
).process(lf)

# Collect — triggers Rust execution across all queued expressions
df = lf.collect()
```

**Constructor reference:**

| Phase class | Key arguments |
|---|---|
| `NormalisePhase` | `skip_patterns: list[str]` — regex patterns; matching rows are left unchanged |
| `ParsePhase` | *(none)* — adds `entry-number` as a 1-based row index |
| `ConcatPhase` | `concats: dict` — `{output_field: {"fields": [...], "separator": str, "prepend": str, "append": str}}` |
| `FilterPhase` | `filters: dict` — `{field_name: regex_pattern}` — rows not matching are dropped |
| `MapPhase` | `fieldnames: list[str]`, `columns: dict` — `{normalised_column: canonical_field}` |
| `PatchPhase` | `patches: dict` — `{field_name: {find_pattern: replacement}}`; use `""` as key to match all fields |
| `HarmonisePhase` | `field_datatype_map: dict`, `dataset: str`, `valid_category_values: dict` |

---

## Comparison with the Legacy Phase Pipeline

The legacy phases in `digital_land/phase/` are built on a Python generator chain. Every phase subclasses `Phase` and overrides `process(stream)`, consuming and yielding one `block` dict at a time:

```python
class Phase:
    def process(self, stream):
        for block in stream:
            yield block
```

Each `block` is a plain dict carrying the raw line, a `row` dict of field→value pairs, the source `resource`, `line-number`, and `entry-number`. Phases are composed by passing one phase's output generator directly as the next phase's input — the entire pipeline is a single-threaded Python iteration.

The Polars phases have no base class. Each phase accepts a `pl.LazyFrame`, adds or mutates columns using Polars expressions, and returns a new `pl.LazyFrame`. Execution is deferred until `.collect()` is called, at which point Polars runs all transformations simultaneously in Rust, optionally across multiple threads and CPU cores.

| Characteristic | Legacy (`digital_land/phase/`) | Polars (`digital_land/phase_polars/`) |
|---|---|---|
| Execution model | Python generator, one row at a time | Rust-backed LazyFrame, all rows at once |
| Memory profile | O(1) — only one row in memory across the pipeline | O(n) — full dataset materialised on `.collect()` |
| Python per row | Yes — every row passes through Python | No — Python only builds the query plan |
| Parallelism | None (single-threaded) | Automatic across columns and row-chunks in Rust |
| Per-row logging | Natural — current row metadata is always available | Not possible during execution (see below) |
| Row identity | `block["entry-number"]` present throughout | Only if `"entry-number"` column is added upstream via `parse.py` |

---

## Issue Logging: the Legacy Approach and Why It Cannot be Directly Ported

### How the legacy pipeline logs issues

The legacy pipeline uses `IssueLog` (in `digital_land/log.py`) as a shared, mutable accumulator. Before processing any field in a block, phases such as `HarmonisePhase` and `PatchPhase` stamp the current row's identity onto the log object:

```python
self.issues.resource = block["resource"]
self.issues.line_number = block["line-number"]
self.issues.entry_number = block["entry-number"]
```

Any subsequent call to `self.issues.log_issue(field, issue_type, value)` is automatically tagged with that row's coordinates and appended to an in-memory list. At the end of the pipeline run the accumulated rows are written to CSV or Parquet.

Phases that use this pattern include:
- **`harmonise.py`** — logs invalid category values, future entry-dates, missing mandatory fields, removed URI prefixes.
- **`patch.py`** — logs every field replacement with the original value and the `"patch"` issue type.
- **`map.py`** — logs column-to-field header mappings once per resource via `ColumnFieldLog`.

### Why this pattern breaks in a LazyFrame pipeline

A `pl.LazyFrame` is an execution plan, not data. When `.with_columns()` or `.filter()` is called, Polars records the transformation as an AST node. Actual row processing happens entirely inside the Rust engine when `.collect()` is called — potentially across multiple threads. There is no Python callback during that execution and no concept of a "current row" that Python code can inspect.

Specific consequences:

- `self.issues.line_number = current_row_index` cannot be called inside a Polars expression because Python never sees individual rows during evaluation.
- `map_elements()` can force a Python callback per cell, but this disables Polars' predicate pushdown, parallelism, and type inference — negating the performance benefit of switching to Polars in the first place, and for a 100k-row dataset with 20 fields this becomes roughly 2 million Python calls.
- Streaming mode (`collect(streaming=True)`) processes batches rather than individual rows — Python still has no per-row hook.

The current `harmonise.py` in this package makes this explicit via a `_NoOpIssues` adapter that silently discards all log calls, noting that per-row telemetry is not yet collected on the Polars path.

### Approaches for re-adding issue logging to the Polars path

#### Option A — Post-collect diff (recommended, fully vectorised)

Collect the relevant columns before and after applying the phase, then compute which cells changed using vectorised operations. This keeps all transformation logic in Rust; Python only processes the diff.

```python
def process(self, lf: pl.LazyFrame):
    before = lf.select(relevant_cols).collect()
    lf_out = self._apply_expressions(lf)
    after = lf_out.select(relevant_cols).collect()

    issue_frames = []
    for col in relevant_cols:
        mask = before[col] != after[col]
        changed = before.filter(mask).select(
            pl.col("entry-number"),
            pl.lit(col).alias("field"),
            pl.col(col).alias("value"),
            pl.lit("patch").alias("issue-type"),
        )
        issue_frames.append(changed)

    issues = pl.concat(issue_frames) if issue_frames else pl.DataFrame()
    return lf_out, issues
```

The `"entry-number"` column added by `parse.py` provides the row identity needed to trace each issue back to its source record — equivalent to `self.issues.entry_number` in the legacy path.

#### Option B — Sentinel columns (single collect, zero extra passes)

During the transformation, add a temporary boolean or value column that marks affected rows. Strip those columns after collecting and convert them into issue rows. This avoids collecting the data twice.

```python
# Inside process():
lf = lf.with_columns(
    replacement_expr.alias(field),
    pl.when(matches).then(pl.col(field)).otherwise(pl.lit(None)).alias(f"_issue_{field}"),
)

# After collect():
df = lf.collect()
issues = (
    df.filter(pl.col(f"_issue_{field}").is_not_null())
    .select(
        pl.col("entry-number"),
        pl.lit(field).alias("field"),
        pl.col(f"_issue_{field}").alias("value"),
        pl.lit("patch").alias("issue-type"),
    )
)
df = df.drop([c for c in df.columns if c.startswith("_issue_")])
```

Sentinel columns add minimal overhead to the query plan and ride through the single Rust execution pass.

#### Option C — Column-level batch list comprehension (fallback for complex datatypes)

The existing `harmonise.py` already uses this pattern for datatype normalisers that cannot be expressed as pure Polars expressions. A column is collected as a Python list, a list comprehension applies the normaliser function (which can write to a side-channel buffer), and the result is written back as a new Series.

This can be extended to capture issues into a `list` buffer during the comprehension, then convert that buffer into a `pl.DataFrame` after the loop. One Python call per cell is unavoidable here, but work is batched at the column level rather than per-row across the full pipeline.

---

## Deployment Status and Infrastructure Constraints

The Polars-enhanced phases were successfully deployed to the development environment for testing. However, the current infrastructure presents several limitations that make it unsuitable for vectorised processing in its present form.

This risk was highlighted in the initial design, where it was noted that the existing infrastructure would likely need to be adjusted or revised to support vectorised processing.

### ECS instance sizing

The ECS instances currently allocated are significantly underpowered for vectorised workloads. The compute profile is better suited to lightweight or intermittent tasks rather than sustained analytical processing. Polars is designed to saturate available CPU and memory; on under-resourced instances the working set can exceed available memory, forcing spills that eliminate the performance advantage.

### CPU credit throttling

CPU credit throttling is reducing effective compute capacity, particularly on burstable instance families (e.g. `t3`/`t4g`). Once the credit balance is exhausted, available CPU drops sharply. This causes slower execution even when the underlying code is capable of much higher throughput, and makes benchmark results unreliable — a run that starts fast may degrade mid-way through a large dataset.

### Multithreading restrictions

The current ECS configuration is restricting multithreading, either through task-level CPU limits, container runtime settings, or scheduling constraints. This prevents Polars from fully utilising available cores. Polars defaults to using all logical CPUs; if the container sees only a fraction of the host's cores, or if thread creation is throttled, the parallel execution that produces the performance gains demonstrated in controlled tests is not available.

### Recommended infrastructure changes

To realise the performance benefits of the Polars path in production the following should be addressed:

- **Upgrade ECS task definitions** to compute-optimised instance types (e.g. `c6i`/`c7g`) sized for the largest expected dataset working set.
- **Move away from burstable instance families** for pipeline tasks, or configure unlimited burst mode if burstable instances are retained.
- **Review container CPU and thread limits** — ensure the ECS task CPU reservation matches the number of vCPUs available to the container, and that no `OMP_NUM_THREADS` / `POLARS_MAX_THREADS` environment variables are artificially capping parallelism.
- **Consider streaming mode** for very large datasets (`collect(streaming=True)`) to reduce peak memory pressure while retaining multi-threaded execution.

---

## Developer Guide: Picking This Up for Further Development

### Understand the execution model before writing any phase logic

The single most important thing to internalise before adding or modifying a phase is that a `pl.LazyFrame` is a query plan, not a container of rows. Calling `.with_columns()`, `.filter()`, or `.select()` does not touch any data — it extends the plan. Data only exists after `.collect()`. If you find yourself wanting to inspect or mutate individual rows inside a phase method, that is a sign the logic needs to be restructured as a Polars expression rather than Python iteration. Reading the [Polars user guide on lazy evaluation](https://docs.pola.rs/user-guide/lazy/) before writing new phase code will save significant debugging time.

### Always ensure `"entry-number"` is present before issue logging

The `parse.py` phase adds an `"entry-number"` column via `lf.with_row_index()`. Every downstream phase that needs to attribute issues to source rows depends on this column existing. When writing a new phase that produces issue output, verify the column is present in the schema before attempting to select it:

```python
schema = lf.collect_schema()
assert "entry-number" in schema.names(), "parse phase must run before this phase"
```

Do not add `entry-number` yourself inside another phase — always rely on `parse.py` having run upstream.

### Implementing a stub phase

Each stub file is an empty module. The convention established by implemented phases is a single class with a `process(self, lf: pl.LazyFrame) -> pl.LazyFrame` method. Start by reading the equivalent legacy phase in `digital_land/phase/` to understand the expected semantics, then translate the per-row logic into Polars expressions. The implemented phases (`normalise.py`, `patch.py`, `filter.py`, `harmonise.py`) are the best reference for tone and structure.

When the legacy phase uses `yield` to conditionally drop rows, the Polars equivalent is `.filter()`. When it mutates a field value, the Polars equivalent is `.with_columns(pl.when(...).then(...).otherwise(pl.col(field)))`.

### Issue logging must be added explicitly — it will not happen automatically

Because `_NoOpIssues` is currently passed to `harmonise.py` and no other phase yet emits issue output, issue logging is entirely absent from the Polars path. When implementing a stub or extending an existing phase to emit issues, choose one of the three approaches described in the [Issue Logging](#issue-logging-the-legacy-approach-and-why-it-cannot-be-directly-ported) section above. The sentinel column approach (Option B) is generally the lowest overhead for phases that can express the "did this row change?" condition as a Polars expression. The post-collect diff approach (Option A) is safer when the change condition is hard to express without collecting the data first.

The issue output should be a `pl.DataFrame` with at minimum the columns: `entry-number`, `field`, `value`, `issue-type`. This mirrors the `IssueLog` schema in `digital_land/log.py`.

### Do not break output schema compatibility with the legacy phases

The primary goal of this package is to produce outputs that are equivalent to the legacy pipeline. Before marking a phase as implemented, run it against the same input used by the legacy phase and compare outputs field-by-field. The test suite in `tests/` contains fixtures that can be used for this. Pay particular attention to:

- Null/empty handling — Polars distinguishes `null` from empty string; the legacy pipeline uses empty string as the canonical "no value" representation.
- Column order — downstream consumers may rely on column ordering; use `.select()` to enforce a stable output schema if needed.
- String types — Polars defaults to `Utf8`; coerce explicitly where schema contracts require it.

### Avoid `map_elements` unless there is no alternative

`map_elements()` (formerly `apply()`) forces Python to be called for each cell, disabling Polars' query optimiser, type inference, and parallelism. It should only be used for logic that genuinely cannot be expressed as a Polars expression (e.g. calling a third-party library that has no vectorised equivalent). `harmonise.py` already uses it as a fallback for certain datatype normalisers — treat those usages as a known cost, not a pattern to replicate where avoidable.

### Load phases are entirely unimplemented

`save_file.py` and `save_database.py` are empty. Before implementing them, confirm the expected output format with the pipeline orchestrator (in `digital_land/commands.py` or equivalent) and ensure they write the same file paths and serialisation formats as the legacy `save.py` and `dump.py` phases. The load phases are the final step before output artifacts are consumed by other services, so any schema or path mismatch here will break downstream dependencies silently.

### Testing approach

- Unit-test each phase in isolation by constructing a small `pl.LazyFrame`, calling `process()`, collecting the result, and asserting on specific column values.
- Cross-validate against the legacy phase using the same CSV fixture where possible — run both pipelines on the same input and assert the outputs are equal after normalising column order and null representation.
- Performance tests should be run locally on hardware with adequate cores (at least 4 vCPUs) rather than in CI or on the current ECS environment, where throttling will produce misleading results (see the [Deployment Status and Infrastructure Constraints](#deployment-status-and-infrastructure-constraints) section).

### Infrastructure prerequisite before benchmarking

Do not treat ECS benchmark results as meaningful until the infrastructure constraints described in the [Deployment Status and Infrastructure Constraints](#deployment-status-and-infrastructure-constraints) section have been addressed. Controlled local benchmarks on adequately sized hardware are currently the only reliable way to measure the performance difference between the legacy and Polars pipelines.

---

## Design Principles

Each phase is designed to be:
- **Modular** — Can be used independently or composed in sequence.
- **Lazy** — Phases operate on Polars `LazyFrame`s, deferring execution until needed.
- **Compatible** — Intended to produce equivalent outputs to the legacy stream-based phases in `digital_land/phase/`.
