#!/usr/bin/env python3
"""
Multi-file performance benchmark: Legacy stream phases (1–9) vs Polars LazyFrame phases (2–9).

Processes every CSV file in tests/integration/data/csv/ (INSPIRE GML-converted
council data), producing per-file timing tables plus an aggregate summary.

Phases benchmarked
──────────────────
  Phase 1  ConvertPhase       (legacy only — no Polars equivalent yet)
  Phase 2  NormalisePhase
  Phase 3  ParsePhase
  Phase 4  ConcatFieldPhase / ConcatPhase
  Phase 5  FilterPhase
  Phase 6  MapPhase
  Phase 7  PatchPhase
  Phase 8  HarmonisePhase

Strategy
────────
Each phase is benchmarked *in isolation*: input data for that phase is fully
materialised beforehand so we measure only the phase's own computation.

  Legacy : list of stream blocks is passed to phase.process(); the generator is
           exhausted and the wall-clock time recorded.

  Polars : collected LazyFrame is passed to phase.process(); result is
           immediately collected to force execution; wall-clock time recorded.

N_RUNS timed repetitions are averaged per phase per file.

Usage
─────
    python tests/integration/phase_polars/test_performance_benchmark_multi.py
    python tests/integration/phase_polars/test_performance_benchmark_multi.py --files 5
    python tests/integration/phase_polars/test_performance_benchmark_multi.py --csv-dir path/to/csvs
"""

import argparse
import sys
import time
import platform
import statistics
from copy import deepcopy
from pathlib import Path
from typing import Tuple, List, Dict


# ── mock cchardet (not installed in this env) so ConvertPhase can be imported ─
class _MockUniversalDetector:
    def __init__(self):
        pass

    def reset(self):
        pass

    def feed(self, _):
        pass

    def close(self):
        pass

    @property
    def done(self):
        return True

    @property
    def result(self):
        return {"encoding": "utf-8"}


sys.modules["cchardet"] = type(sys)("cchardet")
sys.modules["cchardet"].UniversalDetector = _MockUniversalDetector

# ── polars ─────────────────────────────────────────────────────────────────────
import polars as pl

# ── legacy (stream-based) phases ──────────────────────────────────────────────
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.normalise import NormalisePhase as LNormalise
from digital_land.phase.parse import ParsePhase as LParse
from digital_land.phase.concat import ConcatFieldPhase as LConcat
from digital_land.phase.filter import FilterPhase as LFilter
from digital_land.phase.map import MapPhase as LMap
from digital_land.phase.patch import PatchPhase as LPatch
from digital_land.phase.harmonise import HarmonisePhase as LHarmonise

# ── polars phases ──────────────────────────────────────────────────────────────
from digital_land.phase_polars.transform.normalise import NormalisePhase as PNormalise
from digital_land.phase_polars.transform.parse import ParsePhase as PParse
from digital_land.phase_polars.transform.concat import ConcatPhase as PConcat
from digital_land.phase_polars.transform.filter import FilterPhase as PFilter
from digital_land.phase_polars.transform.map import MapPhase as PMap
from digital_land.phase_polars.transform.patch import PatchPhase as PPatch
from digital_land.phase_polars.transform.harmonise import HarmonisePhase as PHarmonise
from digital_land.utils.convert_stream_polarsdf import StreamToPolarsConverter

# ── benchmark configuration ────────────────────────────────────────────────────
N_RUNS = 3
DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"
DATASET = "title-boundary"

CONCAT_CONFIG = {}  # INSPIRE GML schema has no compound reference fields to concatenate
FILTER_CONFIG = {}  # no row filtering — full dataset passes through
COLUMN_MAP = {}  # identity column mapping
PATCH_CONFIG = {}  # no patches (phase still iterates every row)

# INSPIRE GML CSV column names (output of ogr2ogr conversion)
FIELDNAMES = [
    "WKT",
    "gml_id",
    "INSPIREID",
    "LABEL",
    "NATIONALCADASTRALREFERENCE",
    "VALIDFROM",
    "BEGINLIFESPANVERSION",
]

# Datatypes for INSPIRE GML fields
FIELD_DATATYPE_MAP = {
    "WKT": "multipolygon",
    "gml_id": "string",
    "INSPIREID": "string",
    "LABEL": "string",
    "NATIONALCADASTRALREFERENCE": "string",
    "VALIDFROM": "datetime",
    "BEGINLIFESPANVERSION": "datetime",
}


# ── no-op issues stub ─────────────────────────────────────────────────────────
class _NoOpIssues:
    resource = ""
    line_number = 0
    entry_number = 0
    fieldname = ""

    def log_issue(self, *_a, **_k):
        pass

    def log(self, *_a, **_k):
        pass


# ── phase descriptor factory ──────────────────────────────────────────────────
# Returns (phase_number, display_label, legacy_factory, polars_factory).
# Factories are zero-arg callables that return a ready phase instance.
# polars_factory is None for phases without a Polars equivalent yet.


def _make_phase_descriptors(csv_path: Path) -> list:
    return [
        (
            1,
            "ConvertPhase",
            lambda p=csv_path: ConvertPhase(path=str(p)),
            None,  # not yet refactored to Polars
        ),
        (
            2,
            "NormalisePhase",
            lambda: LNormalise(),
            lambda: PNormalise(),
        ),
        (
            3,
            "ParsePhase",
            lambda: LParse(),
            lambda: PParse(),
        ),
        (
            4,
            "ConcatFieldPhase",
            lambda: LConcat(concats=CONCAT_CONFIG),
            lambda: PConcat(concats=CONCAT_CONFIG),
        ),
        (
            5,
            "FilterPhase",
            lambda: LFilter(filters=FILTER_CONFIG),
            lambda: PFilter(filters=FILTER_CONFIG),
        ),
        (
            6,
            "MapPhase",
            lambda: LMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP),
            lambda: PMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP),
        ),
        (
            7,
            "PatchPhase",
            lambda: LPatch(issues=_NoOpIssues(), patches=PATCH_CONFIG),
            lambda: PPatch(patches=PATCH_CONFIG),
        ),
        (
            8,
            "HarmonisePhase",
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

    We deepcopy raw_blocks so that ParsePhase's in-place mutation (it deletes
    the 'line' key from each block dict) never corrupts the shared source list.
    """
    blocks = deepcopy(raw_blocks)

    if phase_index <= 2:
        return blocks  # NormalisePhase receives raw ConvertPhase output

    blocks = list(LNormalise().process(iter(blocks)))
    if phase_index == 3:
        return blocks

    blocks = list(LParse().process(iter(blocks)))
    if phase_index == 4:
        return blocks

    blocks = list(LConcat(concats=CONCAT_CONFIG).process(iter(blocks)))
    if phase_index == 5:
        return blocks

    blocks = list(LFilter(filters=FILTER_CONFIG).process(iter(blocks)))
    if phase_index == 6:
        return blocks

    blocks = list(LMap(fieldnames=FIELDNAMES, columns=COLUMN_MAP).process(iter(blocks)))
    if phase_index == 7:
        return blocks

    blocks = list(
        LPatch(issues=_NoOpIssues(), patches=PATCH_CONFIG).process(iter(blocks))
    )
    return blocks  # input for HarmonisePhase


def _run_polars_phases_up_to(phase_index: int, raw_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Run Polars phases 2..(phase_index - 1) and return a collected+lazy LazyFrame."""
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


# ── single-file benchmark runner ──────────────────────────────────────────────


def run_benchmarks_for_file(
    csv_path: Path, file_index: int, total_files: int
) -> Tuple[dict, int]:
    """Run all phase benchmarks for one CSV file. Returns (results_dict, data_row_count)."""

    print(f"\n  [{file_index}/{total_files}] {csv_path.name}")
    print(f"  Runs: {N_RUNS} per phase\n")

    phase_descriptors = _make_phase_descriptors(csv_path)

    print("  Loading raw stream blocks …")
    raw_blocks = list(ConvertPhase(path=str(csv_path)).process())
    data_row_count = sum(
        1 for b in raw_blocks if "line" in b and b.get("line-number", 1) > 0
    )
    print(f"  {len(raw_blocks):,} blocks loaded  (~{data_row_count:,} data rows)\n")

    print("  Building raw Polars LazyFrame …")
    raw_lf = StreamToPolarsConverter.from_stream(
        ConvertPhase(path=str(csv_path)).process()
    )
    schema_cols = len(raw_lf.collect_schema())
    print(f"  LazyFrame schema: {schema_cols} columns\n")

    results = {}

    for phase_num, label, legacy_factory, polars_factory in phase_descriptors:
        print(f"  ── Phase {phase_num}: {label} ──")

        legacy_times: list[float] = []
        polars_times: list[float] = []

        if polars_factory is None:
            for run in range(1, N_RUNS + 1):
                phase_inst = legacy_factory()
                t0 = time.perf_counter()
                for _ in phase_inst.process():
                    pass
                lt = time.perf_counter() - t0
                legacy_times.append(lt)
                print(f"    run {run}/{N_RUNS}  legacy={lt:.6f}s  polars=N/A")

            results[label] = {
                "phase": phase_num,
                "legacy": legacy_times,
                "polars": None,
                "input_rows": data_row_count,
            }
            print()
            continue

        # Pre-materialise inputs (excluded from timing)
        leg_input = _run_legacy_phases_up_to(phase_num, raw_blocks)
        polars_input = _run_polars_phases_up_to(phase_num, raw_lf)

        for run in range(1, N_RUNS + 1):
            # deepcopy keeps leg_input intact across runs (ParsePhase mutates blocks in-place)
            fresh_legacy = deepcopy(leg_input)
            phase_inst = legacy_factory()
            t0 = time.perf_counter()
            for _ in phase_inst.process(iter(fresh_legacy)):
                pass
            lt = time.perf_counter() - t0
            legacy_times.append(lt)

            phase_inst = polars_factory()
            t0 = time.perf_counter()
            phase_inst.process(polars_input).collect()
            pt = time.perf_counter() - t0
            polars_times.append(pt)

            print(f"    run {run}/{N_RUNS}  legacy={lt:.6f}s  polars={pt:.6f}s")

        results[label] = {
            "phase": phase_num,
            "legacy": legacy_times,
            "polars": polars_times,
            "input_rows": len(leg_input),
        }
        print()

    return results, data_row_count


# ── report formatter ──────────────────────────────────────────────────────────


def _phase_summary_table(results: dict, file_label: str) -> List[str]:
    """Return lines for a per-file summary table."""
    SEP = "─" * 114
    lines = [
        f"  File: {file_label}",
        "  Summary Table  (all times in seconds, averaged over runs)",
        SEP,
        f"  {'Ph':>3}  {'Phase':<22}  {'Leg avg':>11}  {'Leg min':>11}  {'Leg max':>11}  "
        f"{'Pol avg':>11}  {'Pol min':>11}  {'Pol max':>11}  {'Speedup':>8}  Status",
        SEP,
    ]

    total_leg = 0.0
    total_pol = 0.0

    for label, data in results.items():
        lt = data["legacy"]
        pt = data["polars"]
        leg_avg = statistics.mean(lt)

        if pt is None:
            lines.append(
                f"  {data['phase']:>3}  {label:<22}  {leg_avg:>11.6f}  {min(lt):>11.6f}  {max(lt):>11.6f}  "
                f"{'N/A':>11}  {'N/A':>11}  {'N/A':>11}  {'N/A':>7}   legacy only"
            )
            continue

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
            f"  {data['phase']:>3}  {label:<22}  {leg_avg:>11.6f}  {min(lt):>11.6f}  {max(lt):>11.6f}  "
            f"{pol_avg:>11.6f}  {min(pt):>11.6f}  {max(pt):>11.6f}  {speedup:>7.2f}×  {status}"
        )

    lines.append(SEP)
    if total_pol > 0:
        total_speedup = total_leg / total_pol
        lines.append(
            f"  {'':>3}  {'TOTAL  (phases 2–8)':<22}  {total_leg:>11.6f}  {'':>11}  {'':>11}  "
            f"{total_pol:>11.6f}  {'':>11}  {'':>11}  {total_speedup:>7.2f}×"
        )
        lines.append(SEP)

    return lines


def render_report(
    all_results: List[Tuple[str, Dict, int]], csv_dir: Path
) -> str:  # noqa: C901
    SEP = "─" * 114
    DSEP = "═" * 114

    total_rows = sum(rc for _, _, rc in all_results)
    lines: list[str] = []

    lines += [
        "",
        DSEP,
        "  MULTI-FILE PERFORMANCE BENCHMARK REPORT",
        "  Legacy Stream Phases (1–9)  vs  Polars LazyFrame Phases (2–9)",
        DSEP,
        "",
        f"  CSV directory : {csv_dir}",
        f"  Files         : {len(all_results)}",
        f"  Total rows    : {total_rows:,}",
        f"  Runs/phase    : {N_RUNS}",
        f"  Platform      : {platform.platform()}",
        f"  Processor     : {platform.processor() or 'unknown'}",
        f"  Python        : {platform.python_version()}",
        f"  Polars        : {pl.__version__}",
        "",
    ]

    # ── per-file tables ────────────────────────────────────────────────────────
    lines += [DSEP, "  PER-FILE RESULTS", DSEP, ""]

    for file_name, results, row_count in all_results:
        lines += _phase_summary_table(results, f"{file_name}  ({row_count:,} rows)")
        lines.append("")

    # ── aggregate summary ─────────────────────────────────────────────────────
    lines += [DSEP, "  AGGREGATE SUMMARY  (sum and avg/file across all files)", DSEP]

    # Collect per-phase totals
    phase_totals: dict[str, dict] = {}
    for _, results, _ in all_results:
        for label, data in results.items():
            if label not in phase_totals:
                phase_totals[label] = {
                    "phase": data["phase"],
                    "legacy_sum": 0.0,
                    "polars_sum": 0.0 if data["polars"] is not None else None,
                    "files": 0,
                }
            entry = phase_totals[label]
            entry["legacy_sum"] += statistics.mean(data["legacy"])
            entry["files"] += 1
            if data["polars"] is not None:
                if entry["polars_sum"] is None:
                    entry["polars_sum"] = 0.0
                entry["polars_sum"] += statistics.mean(data["polars"])

    n_files = len(all_results)

    lines += [
        "",
        f"  {'Ph':>3}  {'Phase':<22}  {'Leg sum':>11}  {'Leg avg/f':>11}  "
        f"{'Pol sum':>11}  {'Pol avg/f':>11}  {'Speedup':>8}  Status",
        SEP,
    ]

    grand_leg = 0.0
    grand_pol = 0.0

    for label, totals in phase_totals.items():
        leg_sum = totals["legacy_sum"]
        pol_sum = totals["polars_sum"]
        leg_avg = leg_sum / n_files

        if pol_sum is None:
            lines.append(
                f"  {totals['phase']:>3}  {label:<22}  {leg_sum:>11.6f}  {leg_avg:>11.6f}  "
                f"{'N/A':>11}  {'N/A':>11}  {'N/A':>7}   legacy only"
            )
            continue

        pol_avg = pol_sum / n_files
        speedup = leg_sum / pol_sum if pol_sum > 0 else float("inf")
        grand_leg += leg_sum
        grand_pol += pol_sum

        if speedup < 0.90:
            status = "⚠  REGRESSION"
        elif speedup >= 5.0:
            status = "🚀 FAST"
        elif speedup >= 2.0:
            status = "✓  IMPROVED"
        else:
            status = "~  SIMILAR"

        lines.append(
            f"  {totals['phase']:>3}  {label:<22}  {leg_sum:>11.6f}  {leg_avg:>11.6f}  "
            f"{pol_sum:>11.6f}  {pol_avg:>11.6f}  {speedup:>7.2f}×  {status}"
        )

    lines.append(SEP)
    if grand_pol > 0:
        grand_speedup = grand_leg / grand_pol
        lines += [
            f"  {'':>3}  {'GRAND TOTAL (ph 2–8)':<22}  {grand_leg:>11.6f}  {grand_leg / n_files:>11.6f}  "
            f"{grand_pol:>11.6f}  {grand_pol / n_files:>11.6f}  {grand_speedup:>7.2f}×",
            SEP,
            "",
            f"  Overall pipeline speedup (phases 2–8): {grand_speedup:.2f}×",
            f"  Legacy total: {grand_leg:.6f}s  |  Polars total: {grand_pol:.6f}s",
            f"  Avg per file: legacy={grand_leg / n_files:.6f}s  polars={grand_pol / n_files:.6f}s",
        ]

    lines += ["", DSEP, ""]

    return "\n".join(lines)


# ── entry point ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Multi-file benchmark: Legacy phases vs Polars phases"
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=CSV_DIR,
        help=f"Directory containing CSV files to benchmark (default: {CSV_DIR})",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=None,
        metavar="N",
        help="Limit to the first N CSV files (default: all)",
    )
    args = parser.parse_args()

    csv_files = sorted(args.csv_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {args.csv_dir}", file=sys.stderr)
        sys.exit(1)

    if args.files is not None:
        csv_files = csv_files[: args.files]

    print("\n" + "═" * 60)
    print("  Multi-File Phase Performance Benchmark (1–8)")
    print("═" * 60)
    print(f"  CSV directory : {args.csv_dir}")
    print(f"  Files to run  : {len(csv_files)}")

    all_results: list[tuple[str, dict, int]] = []

    for idx, csv_path in enumerate(csv_files, start=1):
        results, row_count = run_benchmarks_for_file(csv_path, idx, len(csv_files))
        all_results.append((csv_path.name, results, row_count))

    report = render_report(all_results, args.csv_dir)
    print(report)

    output_path = DATA_DIR / "benchmark_report_multi.txt"
    output_path.write_text(report, encoding="utf-8")
    print(f"Report saved → {output_path}")


if __name__ == "__main__":
    main()
