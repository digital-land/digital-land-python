"""
Acceptance test: Compare legacy (stream-based) and polars pipeline outputs
up to and including the harmonise phase.

Both implementations receive the SAME input CSV and the SAME pipeline
configuration.  After running:

    ConvertPhase → NormalisePhase → ParsePhase → ConcatFieldPhase →
    FilterPhase → MapPhase → PatchPhase → HarmonisePhase

we collect the rows produced by each implementation and compare them
field-by-field.  This catches any drift between the two code paths on
real (sampled) data.

Requires:  polars  (pip install polars)

Run with:
    pytest tests/acceptance/test_harmonise_comparison.py -v
"""

import csv
import io
import os
import tempfile
from collections import OrderedDict
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Legacy (stream-based) phases
# ---------------------------------------------------------------------------
from digital_land.phase.convert import ConvertPhase as LegacyConvertPhase
from digital_land.phase.normalise import NormalisePhase as LegacyNormalisePhase
from digital_land.phase.parse import ParsePhase as LegacyParsePhase
from digital_land.phase.concat import ConcatFieldPhase as LegacyConcatPhase
from digital_land.phase.filter import FilterPhase as LegacyFilterPhase
from digital_land.phase.map import MapPhase as LegacyMapPhase
from digital_land.phase.patch import PatchPhase as LegacyPatchPhase
from digital_land.phase.harmonise import HarmonisePhase as LegacyHarmonisePhase
from digital_land.pipeline import chain_phases

from digital_land.log import IssueLog, ColumnFieldLog

# ---------------------------------------------------------------------------
# Polars-based phases
# ---------------------------------------------------------------------------
try:
    import polars as pl
    from digital_land.phase_polars.transform.normalise import (
        NormalisePhase as PolarsNormalisePhase,
    )
    from digital_land.phase_polars.transform.parse import (
        ParsePhase as PolarsParsePhase,
    )
    from digital_land.phase_polars.transform.concat import (
        ConcatPhase as PolarsConcatPhase,
    )
    from digital_land.phase_polars.transform.filter import (
        FilterPhase as PolarsFilterPhase,
    )
    from digital_land.phase_polars.transform.map import (
        MapPhase as PolarsMapPhase,
    )
    from digital_land.phase_polars.transform.patch import (
        PatchPhase as PolarsPatchPhase,
    )
    from digital_land.phase_polars.transform.harmonise import (
        HarmonisePhase as PolarsHarmonisePhase,
    )
    from digital_land.utils.convert_stream_polarsdf import StreamToPolarsConverter

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

pytestmark = pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_DATA = REPO_ROOT / "tests" / "data"
SPECIFICATION_DIR = TEST_DATA / "specification"
PIPELINE_DIR = TEST_DATA / "pipeline"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _field_datatype_map_from_spec(spec_dir: Path) -> dict:
    """Build a {field_name: datatype_name} map from the test specification."""
    mapping = {}
    with open(spec_dir / "field.csv", newline="") as f:
        for row in csv.DictReader(f):
            if row["field"] and row["datatype"]:
                mapping[row["field"]] = row["datatype"]
    return mapping


def _load_pipeline_config(pipeline_dir: Path, dataset: str):
    """Load pipeline CSVs (column, concat, patch, default, skip, transform)
    keyed on the given dataset name. Returns a dict of config tables."""
    config = {
        "columns": {},
        "concats": {},
        "patches": {},
        "skip_patterns": [],
        "filters": {},
        "default_fields": {},
    }

    # column.csv
    col_path = pipeline_dir / "column.csv"
    if col_path.exists():
        for row in csv.DictReader(open(col_path, newline="")):
            ds = row.get("dataset", "") or row.get("pipeline", "")
            if ds and ds != dataset:
                continue
            pattern = row.get("column", "") or row.get("pattern", "")
            value = row.get("field", "") or row.get("value", "")
            if pattern and value:
                config["columns"][pattern] = value

    # concat.csv
    cat_path = pipeline_dir / "concat.csv"
    if cat_path.exists():
        for row in csv.DictReader(open(cat_path, newline="")):
            ds = row.get("dataset", "") or row.get("pipeline", "")
            if ds and ds != dataset:
                continue
            config["concats"][row["field"]] = {
                "fields": row["fields"].split(";"),
                "separator": row["separator"],
                "prepend": row.get("prepend", ""),
                "append": row.get("append", ""),
            }

    # patch.csv
    pat_path = pipeline_dir / "patch.csv"
    if pat_path.exists():
        for row in csv.DictReader(open(pat_path, newline="")):
            ds = row.get("dataset", "") or row.get("pipeline", "")
            if ds and ds != dataset:
                continue
            field = row.get("field", "")
            record = config["patches"].setdefault(field, {})
            record[row["pattern"]] = row["value"]

    # skip.csv
    skip_path = pipeline_dir / "skip.csv"
    if skip_path.exists():
        for row in csv.DictReader(open(skip_path, newline="")):
            ds = row.get("dataset", "") or row.get("pipeline", "")
            if ds and ds != dataset:
                continue
            config["skip_patterns"].append(row["pattern"])

    # default.csv
    def_path = pipeline_dir / "default.csv"
    if def_path.exists():
        for row in csv.DictReader(open(def_path, newline="")):
            ds = row.get("dataset", "") or row.get("pipeline", "")
            if ds and ds != dataset:
                continue
            config["default_fields"][row["field"]] = row["default-field"]

    return config


def _schema_fieldnames(spec_dir: Path, schema: str) -> list:
    """Return sorted field names for a schema from the test specification."""
    fields = []
    with open(spec_dir / "schema-field.csv", newline="") as f:
        for row in csv.DictReader(f):
            if row["schema"] == schema:
                fields.append(row["field"])
    return sorted(fields)


def _run_legacy_pipeline(
    csv_path: str,
    fieldnames: list,
    columns: dict,
    concats: dict,
    patches: dict,
    filters: dict,
    skip_patterns: list,
    field_datatype_map: dict,
    dataset: str,
    valid_category_values: dict,
) -> list[dict]:
    """Run the legacy stream pipeline up to & including harmonise.

    Returns a list of row dicts.
    """
    issue_log = IssueLog(dataset=dataset, resource="test-resource")
    column_field_log = ColumnFieldLog(dataset=dataset, resource="test-resource")

    phases = [
        LegacyConvertPhase(path=csv_path),
        LegacyNormalisePhase(skip_patterns=skip_patterns),
        LegacyParsePhase(),
        LegacyConcatPhase(concats=concats, log=column_field_log),
        LegacyFilterPhase(filters=filters),
        LegacyMapPhase(
            fieldnames=fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        LegacyFilterPhase(filters=filters),
        LegacyPatchPhase(issues=issue_log, patches=patches),
        LegacyHarmonisePhase(
            field_datatype_map=field_datatype_map,
            issues=issue_log,
            dataset=dataset,
            valid_category_values=valid_category_values,
        ),
    ]

    pipeline = chain_phases(phases)
    rows = []
    for block in pipeline(None):
        rows.append(dict(block["row"]))
    return rows, issue_log


def _run_polars_pipeline(
    csv_path: str,
    fieldnames: list,
    columns: dict,
    concats: dict,
    patches: dict,
    filters: dict,
    skip_patterns: list,
    field_datatype_map: dict,
    dataset: str,
    valid_category_values: dict,
) -> list[dict]:
    """Run the polars pipeline up to & including harmonise.

    Uses the legacy ConvertPhase to produce a stream, converts it to a
    LazyFrame via StreamToPolarsConverter, then applies polars phases.

    Returns a list of row dicts (all values as strings to match legacy).
    """
    # Step 1: Use legacy ConvertPhase to load into a stream, then convert
    convert = LegacyConvertPhase(path=csv_path)
    stream = convert.process(None)
    lf = StreamToPolarsConverter.from_stream(stream)

    # Step 2: Chain polars phases
    column_field_log = ColumnFieldLog(dataset=dataset, resource="test-resource")

    lf = PolarsNormalisePhase(skip_patterns=skip_patterns).process(lf)
    lf = PolarsParsePhase().process(lf)
    lf = PolarsConcatPhase(concats=concats, log=column_field_log).process(lf)
    lf = PolarsFilterPhase(filters=filters).process(lf)
    lf = PolarsMapPhase(
        fieldnames=fieldnames,
        columns=columns,
        log=column_field_log,
    ).process(lf)
    lf = PolarsFilterPhase(filters=filters).process(lf)
    lf = PolarsPatchPhase(patches=patches).process(lf)
    lf = PolarsHarmonisePhase(
        field_datatype_map=field_datatype_map,
        dataset=dataset,
        valid_category_values=valid_category_values,
    ).process(lf)

    # Step 3: Collect rows as list of dicts, using _stringify_value for
    # clean null/float handling that matches legacy conventions.
    from digital_land.utils.convert_polarsdf_stream import _stringify_value

    df = lf.collect()
    rows = []
    for row_dict in df.to_dicts():
        rows.append({k: _stringify_value(v) for k, v in row_dict.items()})
    return rows


# ---------------------------------------------------------------------------
# Comparison helper
# ---------------------------------------------------------------------------


def _normalise_value(val: str) -> str:
    """Normalise a string value for comparison purposes.

    Handles the serialisation difference where polars may produce
    '90.0' and legacy produces '90' for the same underlying number.
    """
    if val == "" or val is None:
        return ""
    val = str(val).strip()
    if not val:
        return ""
    # Try to normalise numeric representations
    try:
        f = float(val)
        # NaN check
        if f != f:
            return ""
        # If it's a whole number, drop the decimal
        if f == int(f):
            return str(int(f))
        return str(f)
    except (ValueError, OverflowError):
        return val


def compare_outputs(legacy_rows, polars_rows, fields_to_compare=None):
    """Compare legacy and polars outputs row-by-row.

    Numeric values are normalised so that '90' and '90.0' are treated
    as equal.  Non-numeric values (dates, strings) are compared exactly.

    Returns a report dict with summary and per-row diffs.
    """
    report = {
        "legacy_row_count": len(legacy_rows),
        "polars_row_count": len(polars_rows),
        "row_count_match": len(legacy_rows) == len(polars_rows),
        "diffs": [],
    }

    max_rows = max(len(legacy_rows), len(polars_rows))
    for i in range(max_rows):
        row_diff = {"row": i + 1, "field_diffs": []}

        if i >= len(legacy_rows):
            row_diff["error"] = "missing in legacy output"
            report["diffs"].append(row_diff)
            continue
        if i >= len(polars_rows):
            row_diff["error"] = "missing in polars output"
            report["diffs"].append(row_diff)
            continue

        legacy_row = legacy_rows[i]
        polars_row = polars_rows[i]

        # Determine fields to compare
        if fields_to_compare:
            fields = fields_to_compare
        else:
            fields = sorted(set(legacy_row.keys()) & set(polars_row.keys()))

        for field in fields:
            lv = legacy_row.get(field, "")
            pv = polars_row.get(field, "")

            # Normalise for comparison: strip, convert None → ""
            lv = str(lv).strip() if lv is not None else ""
            pv = str(pv).strip() if pv is not None else ""

            # Normalise numeric representations so 90 == 90.0
            if lv != pv and _normalise_value(lv) == _normalise_value(pv):
                continue

            if lv != pv:
                row_diff["field_diffs"].append(
                    {"field": field, "legacy": lv, "polars": pv}
                )

        if row_diff["field_diffs"]:
            report["diffs"].append(row_diff)

    report["all_match"] = len(report["diffs"]) == 0 and report["row_count_match"]
    return report


def format_report(report: dict) -> str:
    """Pretty-print a comparison report for test failure messages."""
    lines = [
        f"Row counts  — legacy: {report['legacy_row_count']}, polars: {report['polars_row_count']}",
        f"Row count match: {report['row_count_match']}",
        f"Total rows with diffs: {len(report['diffs'])}",
        "",
    ]
    for diff in report["diffs"][:20]:  # limit output
        if "error" in diff:
            lines.append(f"  Row {diff['row']}: {diff['error']}")
        else:
            lines.append(f"  Row {diff['row']}:")
            for fd in diff["field_diffs"]:
                lines.append(
                    f"    {fd['field']}: legacy={fd['legacy']!r}  polars={fd['polars']!r}"
                )
    if len(report["diffs"]) > 20:
        lines.append(f"  ... and {len(report['diffs']) - 20} more rows with diffs")
    return "\n".join(lines)


# ===========================================================================
# Test fixtures
# ===========================================================================


@pytest.fixture
def field_datatype_map():
    """Field → datatype map from the test specification."""
    return _field_datatype_map_from_spec(SPECIFICATION_DIR)


@pytest.fixture
def schema_three_fieldnames():
    """Sorted fieldnames for schema-three in the test specification."""
    return _schema_fieldnames(SPECIFICATION_DIR, "schema-three")


# ===========================================================================
# Test: e2e.csv with pipeline-three / schema-three configuration
# ===========================================================================


class TestHarmoniseComparison_E2E:
    """Compare legacy vs polars pipeline through harmonise using the
    existing e2e.csv test data and pipeline-three configuration."""

    @pytest.fixture
    def csv_path(self):
        return str(TEST_DATA / "resource_examples" / "e2e.csv")

    @pytest.fixture
    def pipeline_config(self):
        return _load_pipeline_config(PIPELINE_DIR, "pipeline-three")

    def test_row_count_matches(
        self,
        csv_path,
        pipeline_config,
        schema_three_fieldnames,
        field_datatype_map,
    ):
        """Both implementations produce the same number of output rows."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=pipeline_config["columns"],
            concats=pipeline_config["concats"],
            patches=pipeline_config["patches"],
            filters=pipeline_config["filters"],
            skip_patterns=pipeline_config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=pipeline_config["columns"],
            concats=pipeline_config["concats"],
            patches=pipeline_config["patches"],
            filters=pipeline_config["filters"],
            skip_patterns=pipeline_config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )

        assert len(legacy_rows) == len(polars_rows), (
            f"Row count mismatch: legacy={len(legacy_rows)}, polars={len(polars_rows)}"
        )

    def test_field_values_match(
        self,
        csv_path,
        pipeline_config,
        schema_three_fieldnames,
        field_datatype_map,
    ):
        """All field values match between legacy and polars after harmonise."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=pipeline_config["columns"],
            concats=pipeline_config["concats"],
            patches=pipeline_config["patches"],
            filters=pipeline_config["filters"],
            skip_patterns=pipeline_config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=pipeline_config["columns"],
            concats=pipeline_config["concats"],
            patches=pipeline_config["patches"],
            filters=pipeline_config["filters"],
            skip_patterns=pipeline_config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )

        report = compare_outputs(legacy_rows, polars_rows)
        assert report["all_match"], (
            f"Legacy vs Polars output mismatch:\n{format_report(report)}"
        )


# ===========================================================================
# Test: Buckinghamshire Council sample (title-boundary-like data)
# ===========================================================================


class TestHarmoniseComparison_Buckinghamshire:
    """Compare legacy vs polars pipeline through harmonise using the
    Buckinghamshire Council sample CSV (real cadastral/geometry data)."""

    @pytest.fixture
    def csv_path(self):
        return str(
            REPO_ROOT
            / "tests"
            / "integration"
            / "data"
            / "Buckinghamshire_Council_sample.csv"
        )

    @pytest.fixture
    def fieldnames(self):
        """The fieldnames present in the Buckinghamshire sample that
        also exist in the test specification."""
        return sorted(
            [
                "reference",
                "name",
                "geometry",
                "start-date",
                "entry-date",
                "end-date",
                "prefix",
                "organisation",
                "notes",
            ]
        )

    def test_row_count_matches(
        self,
        csv_path,
        fieldnames,
        field_datatype_map,
    ):
        """Both implementations produce the same number of output rows."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns={},
            concats={},
            patches={},
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="test-dataset",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns={},
            concats={},
            patches={},
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="test-dataset",
            valid_category_values={},
        )

        assert len(legacy_rows) == len(polars_rows), (
            f"Row count mismatch: legacy={len(legacy_rows)}, polars={len(polars_rows)}"
        )

    def test_field_values_match(
        self,
        csv_path,
        fieldnames,
        field_datatype_map,
    ):
        """All field values match between legacy and polars after harmonise."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns={},
            concats={},
            patches={},
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="test-dataset",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns={},
            concats={},
            patches={},
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="test-dataset",
            valid_category_values={},
        )

        report = compare_outputs(legacy_rows, polars_rows)
        assert report["all_match"], (
            f"Legacy vs Polars output mismatch:\n{format_report(report)}"
        )


# ===========================================================================
# Test: Synthetic data with pipeline configuration (column mapping, patches,
#       concatenation, filtering) to exercise each intermediate phase.
# ===========================================================================


class TestHarmoniseComparison_Synthetic:
    """Compare using synthetic data that exercises column mapping,
    concat, filtering, patching, and date harmonisation."""

    @pytest.fixture
    def csv_path(self, tmp_path):
        """Create a small synthetic CSV in a temp directory."""
        data = (
            "ref,Site Name,org,addr,total,Date Recorded,start\n"
            "S001,Town Hall,local-authority-eng:AAA,10 High St,150.5,15/03/2022,2022-01-01\n"
            "S002,Library,local-authority-eng:BBB,20 Main Rd,200,2022/06/30,2022-02-15\n"
            "S003,,local-authority-eng:AAA,30 Oak Lane,0.75,June 2021,\n"
            "S004,Park,local-authority-eng:CCC,not applicable,45,01-Jan-2020,2020-01-01\n"
            "S005,Leisure Centre,local-authority-eng:AAA,50 Elm Drive,320,2023-12-31,2023-01-01\n"
        )
        p = tmp_path / "synthetic_sample.csv"
        p.write_text(data)
        return str(p)

    @pytest.fixture
    def fieldnames(self):
        return sorted(
            [
                "schema-three",
                "name",
                "organisation",
                "address",
                "amount",
                "date",
                "entry-date",
            ]
        )

    @pytest.fixture
    def columns(self):
        """Column mapping similar to the pipeline-three config."""
        return {"adress": "address", "addr": "address"}

    @pytest.fixture
    def concats(self):
        return {}

    @pytest.fixture
    def patches(self):
        return {"address": {"not applicable": "none"}}

    def test_row_count_matches(
        self,
        csv_path,
        fieldnames,
        columns,
        concats,
        patches,
        field_datatype_map,
    ):
        """Both implementations produce the same number of output rows."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns=columns,
            concats=concats,
            patches=patches,
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns=columns,
            concats=concats,
            patches=patches,
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )

        assert len(legacy_rows) == len(polars_rows), (
            f"Row count mismatch: legacy={len(legacy_rows)}, polars={len(polars_rows)}"
        )

    def test_field_values_match(
        self,
        csv_path,
        fieldnames,
        columns,
        concats,
        patches,
        field_datatype_map,
    ):
        """All field values match between legacy and polars after harmonise."""
        legacy_rows, _ = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns=columns,
            concats=concats,
            patches=patches,
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=fieldnames,
            columns=columns,
            concats=concats,
            patches=patches,
            filters={},
            skip_patterns=[],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )

        report = compare_outputs(legacy_rows, polars_rows)
        assert report["all_match"], (
            f"Legacy vs Polars output mismatch:\n{format_report(report)}"
        )


# ===========================================================================
# Diagnostic test: print side-by-side output (always passes)
# ===========================================================================


class TestHarmoniseDiagnostic:
    """A diagnostic test that prints side-by-side output from both
    implementations.  Useful for manual inspection during development.

    Run with:  pytest tests/acceptance/test_harmonise_comparison.py::TestHarmoniseDiagnostic -v -s
    """

    def test_print_comparison(self, field_datatype_map, schema_three_fieldnames):
        """Print legacy vs polars outputs for the e2e.csv data."""
        csv_path = str(TEST_DATA / "resource_examples" / "e2e.csv")
        config = _load_pipeline_config(PIPELINE_DIR, "pipeline-three")

        legacy_rows, issue_log = _run_legacy_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=config["columns"],
            concats=config["concats"],
            patches=config["patches"],
            filters=config["filters"],
            skip_patterns=config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )
        polars_rows = _run_polars_pipeline(
            csv_path=csv_path,
            fieldnames=schema_three_fieldnames,
            columns=config["columns"],
            concats=config["concats"],
            patches=config["patches"],
            filters=config["filters"],
            skip_patterns=config["skip_patterns"],
            field_datatype_map=field_datatype_map,
            dataset="pipeline-three",
            valid_category_values={},
        )

        print("\n" + "=" * 80)
        print("LEGACY → POLARS HARMONISE PHASE COMPARISON")
        print("=" * 80)
        print(f"Input: e2e.csv  |  Dataset: pipeline-three")
        print(f"Legacy rows: {len(legacy_rows)}  |  Polars rows: {len(polars_rows)}")

        report = compare_outputs(legacy_rows, polars_rows)

        if report["all_match"]:
            print("\n✓ ALL ROWS MATCH")
        else:
            print(f"\n✗ DIFFERENCES FOUND")
            print(format_report(report))

        # Also print a sample of rows
        print("\n--- Legacy output (first 3 rows) ---")
        for i, row in enumerate(legacy_rows[:3]):
            print(f"  Row {i + 1}: {dict(row)}")

        print("\n--- Polars output (first 3 rows) ---")
        for i, row in enumerate(polars_rows[:3]):
            print(f"  Row {i + 1}: {dict(row)}")

        # Print issues logged by legacy pipeline
        if issue_log.rows:
            print(f"\n--- Legacy issues ({len(issue_log.rows)}) ---")
            for issue in issue_log.rows[:10]:
                print(
                    f"  [{issue['issue-type']}] {issue['field']}: {issue['value']!r}"
                )

        print("=" * 80)
