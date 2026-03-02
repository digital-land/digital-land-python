#!/usr/bin/env python3
"""
Focused performance benchmark for the Polars HarmonisePhase.

Profiles each internal step of HarmonisePhase independently so you can see
exactly where time is spent and whether optimisations have an impact.

Profiled steps
──────────────
  1  _harmonise_categorical_fields
  2  _harmonise_field_values          (datatype normalisation)
  3  _remove_future_dates
  4  _process_point_geometry           (GeoX/GeoY CRS conversion)
  5  _add_typology_curies
  6  _check_mandatory_fields
  7  _process_wikipedia_urls
  *  process()                         (full end-to-end)

Also includes a legacy vs polars comparison for the full phase.

Usage
─────
    python tests/integration/phase_polars/test_harmonise_benchmark.py
    python tests/integration/phase_polars/test_harmonise_benchmark.py --sample   # 8-row sample for quick smoke tests
"""

import sys
import time
import platform
import statistics
from copy import deepcopy
from pathlib import Path

# ── mock cchardet so ConvertPhase can be imported ─────────────────────────────
class _MockUniversalDetector:
    def __init__(self): pass
    def reset(self): pass
    def feed(self, _): pass
    def close(self): pass
    @property
    def done(self): return True
    @property
    def result(self): return {"encoding": "utf-8"}

sys.modules["cchardet"] = type(sys)("cchardet")
sys.modules["cchardet"].UniversalDetector = _MockUniversalDetector

import polars as pl

from digital_land.phase.convert   import ConvertPhase
from digital_land.phase.normalise import NormalisePhase   as LNormalise
from digital_land.phase.parse     import ParsePhase       as LParse
from digital_land.phase.concat    import ConcatFieldPhase as LConcat
from digital_land.phase.filter    import FilterPhase      as LFilter
from digital_land.phase.map       import MapPhase         as LMap
from digital_land.phase.patch     import PatchPhase       as LPatch
from digital_land.phase.harmonise import HarmonisePhase   as LHarmonise

from digital_land.phase_polars.transform.normalise  import NormalisePhase  as PNormalise
from digital_land.phase_polars.transform.parse      import ParsePhase      as PParse
from digital_land.phase_polars.transform.concat     import ConcatPhase     as PConcat
from digital_land.phase_polars.transform.filter     import FilterPhase     as PFilter
from digital_land.phase_polars.transform.map        import MapPhase        as PMap
from digital_land.phase_polars.transform.patch      import PatchPhase      as PPatch
from digital_land.phase_polars.transform.harmonise  import HarmonisePhase  as PHarmonise
from digital_land.utils.convert_stream_polarsdf     import StreamToPolarsConverter

# ── configuration ─────────────────────────────────────────────────────────────
N_RUNS   = 5
DATA_DIR = Path(__file__).parent.parent / "data"
FULL_CSV = DATA_DIR / "Buckinghamshire_Council.csv"
SAMPLE_CSV = DATA_DIR / "Buckinghamshire_Council_sample.csv"
DATASET  = "title-boundary"

CONCAT_CONFIG = {
    "full-reference": {
        "fields": ["prefix", "reference"],
        "separator": "-",
        "prepend": "",
        "append": "",
    }
}
FILTER_CONFIG = {}
FIELDNAMES = [
    "reference", "name", "national-cadastral-reference", "geometry",
    "start-date", "entry-date", "end-date", "prefix", "organisation", "notes",
]
COLUMN_MAP = {}
PATCH_CONFIG = {}
FIELD_DATATYPE_MAP = {
    "reference":                    "string",
    "name":                         "string",
    "national-cadastral-reference": "string",
    "geometry":                     "multipolygon",
    "start-date":                   "datetime",
    "entry-date":                   "datetime",
    "end-date":                     "datetime",
    "prefix":                       "string",
    "organisation":                 "curie",
    "notes":                        "string",
    "full-reference":               "string",
}


class _NoOpIssues:
    resource = ""
    line_number = 0
    entry_number = 0
    fieldname = ""
    def log_issue(self, *_a, **_k): pass
    def log(self, *_a, **_k): pass


# ── data preparation (run phases 2-7 to produce harmonise input) ──────────────

def _prepare_legacy_input(csv_path: Path) -> list:
    """Run legacy phases 2–7 and return materialised blocks for HarmonisePhase."""
    blocks = list(ConvertPhase(path=str(csv_path)).process())
    blocks = list(LNormalise().process(iter(blocks)))
    blocks = list(LParse().process(iter(blocks)))
    blocks = list(LConcat(concats=CONCAT_CONFIG).process(iter(blocks)))
    blocks = list(LFilter(filters=FILTER_CONFIG).process(iter(blocks)))
    blocks = list(LMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP).process(iter(blocks)))
    blocks = list(LPatch(issues=_NoOpIssues(), patches=PATCH_CONFIG).process(iter(blocks)))
    return blocks


def _prepare_polars_input(csv_path: Path) -> pl.LazyFrame:
    """Run polars phases 2–7 and return a collected LazyFrame for HarmonisePhase."""
    raw_lf = StreamToPolarsConverter.from_stream(
        ConvertPhase(path=str(csv_path)).process()
    )
    lf = PNormalise().process(raw_lf).collect().lazy()
    lf = PParse().process(lf).collect().lazy()
    lf = PConcat(concats=CONCAT_CONFIG).process(lf).collect().lazy()
    lf = PFilter(filters=FILTER_CONFIG).process(lf).collect().lazy()
    lf = PMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP).process(lf).collect().lazy()
    lf = PPatch(patches=PATCH_CONFIG).process(lf).collect().lazy()
    return lf


# ── step-level profiler ──────────────────────────────────────────────────────

STEP_METHODS = [
    ("_harmonise_categorical_fields", "Categorical normalisation"),
    ("_harmonise_field_values",       "Datatype normalisation"),
    ("_remove_future_dates",          "Future date removal"),
    ("_process_point_geometry",       "GeoX/GeoY CRS conversion"),
    ("_add_typology_curies",          "Typology CURIE prefixing"),
    ("_check_mandatory_fields",       "Mandatory field checks"),
    ("_process_wikipedia_urls",       "Wikipedia URL stripping"),
]


def _time_fn(fn, *args) -> float:
    """Call fn(*args) and return wall-clock seconds."""
    t0 = time.perf_counter()
    result = fn(*args)
    # Force collect if result is a LazyFrame
    if isinstance(result, pl.LazyFrame):
        result.collect()
    return time.perf_counter() - t0


def profile_polars_steps(polars_input: pl.LazyFrame, n_runs: int) -> dict:
    """Time each internal step of the Polars HarmonisePhase independently."""
    phase = PHarmonise(
        field_datatype_map=FIELD_DATATYPE_MAP,
        dataset=DATASET,
        valid_category_values={},
    )

    # Materialise once so schema introspection is excluded from timing.
    df = polars_input.collect()
    existing_columns = df.columns

    results = {}

    for method_name, label in STEP_METHODS:
        method = getattr(phase, method_name)
        times: list[float] = []

        for _ in range(n_runs):
            # Each step gets a fresh lazy frame from the same collected data.
            lf = df.lazy()
            times.append(_time_fn(method, lf, existing_columns))

        results[label] = {
            "method": method_name,
            "times":  times,
        }

    return results


def benchmark_full_phase(legacy_input: list, polars_input: pl.LazyFrame, n_runs: int) -> dict:
    """Time the full process() for both legacy and polars."""
    legacy_times: list[float] = []
    polars_times: list[float] = []

    for _ in range(n_runs):
        # Legacy
        fresh = deepcopy(legacy_input)
        phase = LHarmonise(
            field_datatype_map=FIELD_DATATYPE_MAP,
            issues=_NoOpIssues(),
            dataset=DATASET,
            valid_category_values={},
        )
        t0 = time.perf_counter()
        for _ in phase.process(iter(fresh)):
            pass
        legacy_times.append(time.perf_counter() - t0)

        # Polars
        phase = PHarmonise(
            field_datatype_map=FIELD_DATATYPE_MAP,
            dataset=DATASET,
            valid_category_values={},
        )
        t0 = time.perf_counter()
        phase.process(polars_input).collect()
        polars_times.append(time.perf_counter() - t0)

    return {"legacy": legacy_times, "polars": polars_times}


# ── report ────────────────────────────────────────────────────────────────────

def render_report(step_results: dict, full_results: dict, row_count: int, csv_name: str) -> str:
    SEP  = "─" * 90
    DSEP = "═" * 90

    lines: list[str] = [
        "",
        DSEP,
        "  HARMONISE PHASE BENCHMARK",
        DSEP,
        "",
        f"  Dataset   : {csv_name}",
        f"  Data rows : {row_count:,}",
        f"  Runs/step : {N_RUNS}",
        f"  Platform  : {platform.platform()}",
        f"  Python    : {platform.python_version()}",
        f"  Polars    : {pl.__version__}",
        "",
    ]

    # ── step-level breakdown ──────────────────────────────────────────────────
    lines += [
        "Polars Step Breakdown  (seconds)",
        SEP,
        f"  {'#':>2}  {'Step':<30}  {'avg':>8}  {'min':>8}  {'max':>8}  {'stdev':>8}  {'% total':>8}",
        SEP,
    ]

    step_avgs = {label: statistics.mean(d["times"]) for label, d in step_results.items()}
    total_step_avg = sum(step_avgs.values())

    for i, (label, data) in enumerate(step_results.items(), 1):
        t = data["times"]
        avg = step_avgs[label]
        pct = (avg / total_step_avg * 100) if total_step_avg > 0 else 0
        sd = statistics.stdev(t) if len(t) > 1 else 0.0
        lines.append(
            f"  {i:>2}  {label:<30}  {avg:>8.4f}  {min(t):>8.4f}  {max(t):>8.4f}  {sd:>8.4f}  {pct:>7.1f}%"
        )

    lines.append(SEP)
    lines.append(f"  {'':>2}  {'SUM OF STEPS':<30}  {total_step_avg:>8.4f}")
    lines.append(SEP)

    # ── full-phase legacy vs polars ───────────────────────────────────────────
    lines += ["", "Full Phase Comparison  (seconds)", SEP]

    leg = full_results["legacy"]
    pol = full_results["polars"]
    leg_avg = statistics.mean(leg)
    pol_avg = statistics.mean(pol)
    speedup = leg_avg / pol_avg if pol_avg > 0 else float("inf")

    lines += [
        f"  Legacy avg  : {leg_avg:.4f}s  (min={min(leg):.4f}  max={max(leg):.4f})",
        f"  Polars avg  : {pol_avg:.4f}s  (min={min(pol):.4f}  max={max(pol):.4f})",
        f"  Speedup     : {speedup:.2f}×",
    ]

    if speedup < 0.90:
        lines.append(f"  Status      : ⚠  REGRESSION ({1/speedup:.2f}× slower)")
    elif speedup >= 5.0:
        lines.append(f"  Status      : 🚀 FAST")
    elif speedup >= 2.0:
        lines.append(f"  Status      : ✓  IMPROVED")
    else:
        lines.append(f"  Status      : ~  SIMILAR")

    lines.append(SEP)

    # ── hotspot analysis ──────────────────────────────────────────────────────
    lines += ["", "Hotspot Analysis", SEP]

    ranked = sorted(step_avgs.items(), key=lambda kv: kv[1], reverse=True)
    for rank, (label, avg) in enumerate(ranked, 1):
        pct = (avg / total_step_avg * 100) if total_step_avg > 0 else 0
        bar = "█" * int(pct / 2)
        lines.append(f"  {rank}. {label:<30}  {avg:>8.4f}s  {pct:>5.1f}%  {bar}")

    overhead = pol_avg - total_step_avg
    if overhead > 0:
        lines.append(f"\n  Overhead (process() - sum of steps): {overhead:.4f}s")
        lines.append(f"  This includes schema checks, entry-number drop, etc.")

    lines += [SEP, ""]

    return "\n".join(lines)


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    use_sample = "--sample" in sys.argv
    csv_path = SAMPLE_CSV if use_sample else FULL_CSV

    if not csv_path.exists():
        print(f"  ERROR: {csv_path} not found.")
        print(f"  Place the data file at {csv_path} and re-run.")
        sys.exit(1)

    print("\n" + "═" * 60)
    print("  Harmonise Phase Benchmark")
    print("═" * 60)
    print(f"\n  Dataset : {csv_path.name}")
    print(f"  Runs    : {N_RUNS}\n")

    # ── prepare inputs (not timed) ────────────────────────────────────────────
    print("  Preparing legacy input (phases 2–7) …")
    legacy_input = _prepare_legacy_input(csv_path)
    row_count = len(legacy_input)
    print(f"  {row_count:,} blocks")

    print("  Preparing polars input (phases 2–7) …")
    polars_input = _prepare_polars_input(csv_path)
    polars_rows = polars_input.collect().height
    print(f"  {polars_rows:,} rows\n")

    # ── profile individual steps ──────────────────────────────────────────────
    print("  Profiling polars HarmonisePhase steps …")
    step_results = profile_polars_steps(polars_input, N_RUNS)
    print("  Done.\n")

    # ── full phase comparison ─────────────────────────────────────────────────
    print("  Benchmarking full phase (legacy vs polars) …")
    full_results = benchmark_full_phase(legacy_input, polars_input, N_RUNS)
    print("  Done.\n")

    # ── report ────────────────────────────────────────────────────────────────
    report = render_report(step_results, full_results, polars_rows, csv_path.name)
    print(report)

    output_path = DATA_DIR / "harmonise_benchmark_report.txt"
    output_path.write_text(report, encoding="utf-8")
    print(f"  Report saved → {output_path}")


if __name__ == "__main__":
    main()
