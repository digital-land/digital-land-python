#!/usr/bin/env python3
"""
Performance benchmark: Legacy stream phases (2–9) vs Polars LazyFrame phases (2–9).

Phases benchmarked
──────────────────
  Phase 2  NormalisePhase
  Phase 3  ParsePhase
  Phase 4  ConcatFieldPhase / ConcatPhase
  Phase 5  FilterPhase
  Phase 6  MapPhase
  Phase 7  PatchPhase
  Phase 8  HarmonisePhase  (phase 9 in the full pipeline)

Strategy
────────
Each phase is benchmarked *in isolation*: input data for that phase is fully
materialised beforehand so we measure only the phase's own computation.

  Legacy : list of stream blocks is passed to phase.process(); the generator is
           exhausted and the wall-clock time recorded.

  Polars : collected LazyFrame is passed to phase.process(); result is
           immediately collected to force execution; wall-clock time recorded.

N_RUNS timed repetitions are averaged per phase.

Usage
─────
    python tests/integration/phase_polars/test_performance_benchmark.py
"""

import sys
import time
import platform
import statistics
from copy import deepcopy
from pathlib import Path

# ── mock cchardet (not installed in this env) so ConvertPhase can be imported ─
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

# ── polars ─────────────────────────────────────────────────────────────────────
import polars as pl

# ── legacy (stream-based) phases ──────────────────────────────────────────────
from digital_land.phase.convert   import ConvertPhase
from digital_land.phase.normalise import NormalisePhase   as LNormalise
from digital_land.phase.parse     import ParsePhase       as LParse
from digital_land.phase.concat    import ConcatFieldPhase as LConcat
from digital_land.phase.filter    import FilterPhase      as LFilter
from digital_land.phase.map       import MapPhase         as LMap
from digital_land.phase.patch     import PatchPhase       as LPatch
from digital_land.phase.harmonise import HarmonisePhase   as LHarmonise

# ── polars phases ──────────────────────────────────────────────────────────────
from digital_land.phase_polars.transform.normalise  import NormalisePhase  as PNormalise
from digital_land.phase_polars.transform.parse      import ParsePhase      as PParse
from digital_land.phase_polars.transform.concat     import ConcatPhase     as PConcat
from digital_land.phase_polars.transform.filter     import FilterPhase     as PFilter
from digital_land.phase_polars.transform.map        import MapPhase        as PMap
from digital_land.phase_polars.transform.patch      import PatchPhase      as PPatch
from digital_land.phase_polars.transform.harmonise  import HarmonisePhase  as PHarmonise
from digital_land.utils.convert_stream_polarsdf     import StreamToPolarsConverter

# ── benchmark configuration ────────────────────────────────────────────────────
N_RUNS   = 3
CSV_PATH = Path(__file__).parent.parent / "data" / "Buckinghamshire_Council.csv"
DATASET  = "title-boundary"

CONCAT_CONFIG = {
    "full-reference": {
        "fields": ["prefix", "reference"],
        "separator": "-",
        "prepend": "",
        "append": "",
    }
}
FILTER_CONFIG = {}   # no row filtering – full dataset passes through
FIELDNAMES    = [
    "reference", "name", "national-cadastral-reference", "geometry",
    "start-date", "entry-date", "end-date", "prefix", "organisation", "notes",
]
COLUMN_MAP    = {}   # identity column mapping
PATCH_CONFIG  = {}   # no patches (phase still iterates every row)

# Datatypes sourced from specification/field.csv; unknown fields default to "string"
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


# ── no-op issues stub ─────────────────────────────────────────────────────────
class _NoOpIssues:
    resource = ""
    line_number = 0
    entry_number = 0
    fieldname = ""
    def log_issue(self, *_a, **_k): pass
    def log(self, *_a, **_k): pass


# ── phase descriptors ─────────────────────────────────────────────────────────
# Each entry: (phase_number, display_label, legacy_factory, polars_factory)
# Factories are zero-arg callables that return a ready phase instance.

PHASE_DESCRIPTORS = [
    (
        2, "NormalisePhase",
        lambda: LNormalise(),
        lambda: PNormalise(),
    ),
    (
        3, "ParsePhase",
        lambda: LParse(),
        lambda: PParse(),
    ),
    (
        4, "ConcatFieldPhase",
        lambda: LConcat(concats=CONCAT_CONFIG),
        lambda: PConcat(concats=CONCAT_CONFIG),
    ),
    (
        5, "FilterPhase",
        lambda: LFilter(filters=FILTER_CONFIG),
        lambda: PFilter(filters=FILTER_CONFIG),
    ),
    (
        6, "MapPhase",
        lambda: LMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP),
        lambda: PMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP),
    ),
    (
        7, "PatchPhase",
        lambda: LPatch(issues=_NoOpIssues(), patches=PATCH_CONFIG),
        lambda: PPatch(patches=PATCH_CONFIG),
    ),
    (
        8, "HarmonisePhase",
        lambda: LHarmonise(
            field_datatype_map=FIELD_DATATYPE_MAP,
            issues=_NoOpIssues(),
            dataset=DATASET,
            valid_category_values={},
        ),
        lambda: PHarmonise(
            field_datatype_map=FIELD_DATATYPE_MAP,
            dataset=DATASET,
            valid_category_values={},
        ),
    ),
]


# ── pre-materialise helpers ───────────────────────────────────────────────────

def _run_legacy_phases_up_to(phase_index: int, raw_blocks: list) -> list:
    """
    Run legacy phases 2..(phase_index - 1) and return materialised blocks.
    phase_index uses the PHASE_DESCRIPTORS numbering (2–8).

    We deepcopy raw_blocks so that ParsePhase's in-place mutation (it deletes
    the 'line' key from each block dict) never corrupts the shared source list.
    """
    blocks = deepcopy(raw_blocks)

    if phase_index <= 2:
        return blocks  # NormalisePhase receives raw ConvertPhase output

    # Phase 2 – Normalise
    blocks = list(LNormalise().process(iter(blocks)))
    if phase_index == 3:
        return blocks

    # Phase 3 – Parse
    blocks = list(LParse().process(iter(blocks)))
    if phase_index == 4:
        return blocks

    # Phase 4 – Concat
    blocks = list(LConcat(concats=CONCAT_CONFIG).process(iter(blocks)))
    if phase_index == 5:
        return blocks

    # Phase 5 – Filter
    blocks = list(LFilter(filters=FILTER_CONFIG).process(iter(blocks)))
    if phase_index == 6:
        return blocks

    # Phase 6 – Map
    blocks = list(LMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP).process(iter(blocks)))
    if phase_index == 7:
        return blocks

    # Phase 7 – Patch
    blocks = list(LPatch(issues=_NoOpIssues(), patches=PATCH_CONFIG).process(iter(blocks)))
    return blocks  # input for HarmonisePhase


def _run_polars_phases_up_to(phase_index: int, raw_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Run Polars phases 2..(phase_index - 1) and return a collected+lazy LazyFrame.
    """
    if phase_index <= 2:
        return raw_lf

    lf = PNormalise().process(raw_lf).collect().lazy()
    if phase_index == 3:
        return lf

    lf = PParse().process(lf).collect().lazy()
    if phase_index == 4:
        return lf

    lf = PConcat(concats=CONCAT_CONFIG).process(lf).collect().lazy()
    if phase_index == 5:
        return lf

    lf = PFilter(filters=FILTER_CONFIG).process(lf).collect().lazy()
    if phase_index == 6:
        return lf

    lf = PMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP).process(lf).collect().lazy()
    if phase_index == 7:
        return lf

    lf = PPatch(patches=PATCH_CONFIG).process(lf).collect().lazy()
    return lf  # input for HarmonisePhase


# ── benchmark runner ──────────────────────────────────────────────────────────

def run_benchmarks() -> tuple[dict, int]:
    """Run all phase benchmarks, return (results_dict, data_row_count)."""

    print(f"\n  Dataset : {CSV_PATH.name}")
    print(f"  Runs    : {N_RUNS} per phase\n")

    # Load raw data once
    print("  Loading raw stream blocks …")
    raw_blocks = list(ConvertPhase(path=str(CSV_PATH)).process())
    data_row_count = sum(
        1 for b in raw_blocks
        if "line" in b and b.get("line-number", 1) > 0
    )
    print(f"  {len(raw_blocks):,} blocks loaded  (~{data_row_count:,} data rows)\n")

    print("  Building raw Polars LazyFrame …")
    raw_lf = StreamToPolarsConverter.from_stream(
        ConvertPhase(path=str(CSV_PATH)).process()
    )
    schema_cols = len(raw_lf.collect_schema())
    print(f"  LazyFrame schema: {schema_cols} columns\n")

    results = {}

    for phase_num, label, legacy_factory, polars_factory in PHASE_DESCRIPTORS:
        print(f"  ── Phase {phase_num}: {label} ──")

        # Pre-materialise inputs (excluded from timing)
        leg_input    = _run_legacy_phases_up_to(phase_num, raw_blocks)
        polars_input = _run_polars_phases_up_to(phase_num, raw_lf)

        legacy_times: list[float] = []
        polars_times: list[float] = []

        for run in range(1, N_RUNS + 1):
            # Legacy: exhaust the generator
            # deepcopy keeps leg_input intact across runs (ParsePhase mutates blocks in-place)
            fresh_legacy = deepcopy(leg_input)
            phase_inst = legacy_factory()
            t0 = time.perf_counter()
            for _ in phase_inst.process(iter(fresh_legacy)):
                pass
            lt = time.perf_counter() - t0
            legacy_times.append(lt)

            # Polars: lazy plan + force collect
            phase_inst = polars_factory()
            t0 = time.perf_counter()
            phase_inst.process(polars_input).collect()
            pt = time.perf_counter() - t0
            polars_times.append(pt)

            print(f"    run {run}/{N_RUNS}  legacy={lt:.3f}s  polars={pt:.3f}s")

        results[label] = {
            "phase":  phase_num,
            "legacy": legacy_times,
            "polars": polars_times,
            "input_rows": len(leg_input),
        }
        print()

    return results, data_row_count


# ── report formatter ──────────────────────────────────────────────────────────

def render_report(results: dict, row_count: int) -> str:  # noqa: C901
    SEP  = "─" * 96
    DSEP = "═" * 96

    lines: list[str] = []

    lines += [
        "",
        DSEP,
        "  PERFORMANCE BENCHMARK REPORT",
        "  Legacy Stream Phases (2–9)  vs  Polars LazyFrame Phases (2–9)",
        DSEP,
        "",
        f"  Dataset   : {CSV_PATH.name}",
        f"  Data rows : {row_count:,}",
        f"  Runs/phase: {N_RUNS}",
        f"  Platform  : {platform.platform()}",
        f"  Processor : {platform.processor() or 'unknown'}",
        f"  Python    : {platform.python_version()}",
        f"  Polars    : {pl.__version__}",
        "",
    ]

    # ── per-phase summary table ────────────────────────────────────────────────
    lines += [
        "Summary Table  (all times in seconds, averaged over runs)",
        SEP,
        f"  {'Ph':>3}  {'Phase':<22}  {'Leg avg':>8}  {'Leg min':>8}  {'Leg max':>8}  "
        f"{'Pol avg':>8}  {'Pol min':>8}  {'Pol max':>8}  {'Speedup':>8}  Status",
        SEP,
    ]

    total_leg = 0.0
    total_pol = 0.0

    for label, data in results.items():
        lt = data["legacy"]
        pt = data["polars"]
        leg_avg = statistics.mean(lt)
        pol_avg = statistics.mean(pt)
        speedup = leg_avg / pol_avg if pol_avg > 0 else float("inf")
        total_leg += leg_avg
        total_pol += pol_avg

        if speedup < 0.90:
            status = "⚠  REGRESSION"
        elif speedup >= 5.0:
            status = "🚀 FAST"
        elif speedup >= 2.0:
            status = "✓  IMPROVED"
        else:
            status = "~  SIMILAR"

        lines.append(
            f"  {data['phase']:>3}  {label:<22}  {leg_avg:>8.3f}  {min(lt):>8.3f}  {max(lt):>8.3f}  "
            f"{pol_avg:>8.3f}  {min(pt):>8.3f}  {max(pt):>8.3f}  {speedup:>7.2f}×  {status}"
        )

    lines.append(SEP)
    total_speedup = total_leg / total_pol if total_pol > 0 else float("inf")
    lines.append(
        f"  {'':>3}  {'TOTAL  (phases 2–9)':<22}  {total_leg:>8.3f}  {'':>8}  {'':>8}  "
        f"{total_pol:>8.3f}  {'':>8}  {'':>8}  {total_speedup:>7.2f}×"
    )
    lines.append(SEP)

    # ── per-run detail table ───────────────────────────────────────────────────
    lines += [
        "",
        "Per-run Timing Detail  (seconds)",
        SEP,
    ]
    run_header = f"  {'Phase':<22}"
    for r in range(1, N_RUNS + 1):
        run_header += f"   Leg {r}   Pol {r}"
    lines.append(run_header)
    lines.append(SEP)

    for label, data in results.items():
        row = f"  {label:<22}"
        for lt, pt in zip(data["legacy"], data["polars"]):
            row += f"  {lt:7.3f}  {pt:7.3f}"
        lines.append(row)
    lines.append(SEP)

    # ── observations ──────────────────────────────────────────────────────────
    lines += ["", "Observations", SEP]

    regressions = []
    improvements = []
    similar = []

    for label, data in results.items():
        leg_avg = statistics.mean(data["legacy"])
        pol_avg = statistics.mean(data["polars"])
        speedup = leg_avg / pol_avg if pol_avg > 0 else float("inf")

        if speedup < 0.90:
            entry = (
                f"  ⚠  Phase {data['phase']} {label}: Polars is {1/speedup:.2f}× SLOWER than legacy "
                f"[polars={pol_avg:.3f}s  legacy={leg_avg:.3f}s]. Investigate further – "
                f"possible overhead from LazyFrame materialisation or DuckDB usage in this phase."
            )
            regressions.append(entry)
        elif speedup >= 2.0:
            entry = (
                f"  ✓  Phase {data['phase']} {label}: Polars is {speedup:.2f}× faster "
                f"[polars={pol_avg:.3f}s  legacy={leg_avg:.3f}s]."
            )
            improvements.append(entry)
        else:
            entry = (
                f"  ~  Phase {data['phase']} {label}: Performance similar ({speedup:.2f}× speedup). "
                f"[polars={pol_avg:.3f}s  legacy={leg_avg:.3f}s]."
            )
            similar.append(entry)

    if regressions:
        lines.append("  Regressions (Polars slower):")
        lines += regressions
    if improvements:
        lines.append("  Improvements (Polars faster):")
        lines += improvements
    if similar:
        lines.append("  Comparable performance:")
        lines += similar

    lines += [
        SEP,
        "",
        f"  Overall pipeline speedup (phases 2–9): {total_speedup:.2f}×",
        f"  Legacy total: {total_leg:.3f}s  |  Polars total: {total_pol:.3f}s",
        "",
        DSEP,
        "",
    ]

    return "\n".join(lines)


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  Phase Performance Benchmark (2–9)")
    print("═" * 60)

    results, row_count = run_benchmarks()
    report = render_report(results, row_count)

    print(report)

    output_path = CSV_PATH.parent / "benchmark_report.txt"
    output_path.write_text(report, encoding="utf-8")
    print(f"Report saved → {output_path}")


if __name__ == "__main__":
    main()
