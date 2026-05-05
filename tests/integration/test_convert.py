#!/usr/bin/env -S py.test -svv
import csv
import pathlib
import pytest
import xlsxwriter
from shapely import wkt as shapely_wkt

from click.testing import CliRunner
from digital_land.cli import cli

csv.field_size_limit(10_000_000)

# Tolerance for bounding box comparison in degrees (~1km)
BBOX_TOLERANCE = 0.01


def _read_csv(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        cols = reader.fieldnames or []
    return rows, cols


def _bounding_box(rows, wkt_col):
    bounds = []
    for row in rows:
        if row.get(wkt_col):
            geom = shapely_wkt.loads(row[wkt_col])
            bounds.append(geom.bounds)  # (minx, miny, maxx, maxy)
    if not bounds:
        return None
    return (
        min(b[0] for b in bounds),
        min(b[1] for b in bounds),
        max(b[2] for b in bounds),
        max(b[3] for b in bounds),
    )


def _assert_convert_properties(output_file, golden_file):
    golden_rows, golden_cols = _read_csv(golden_file)
    output_rows, output_cols = _read_csv(output_file)

    # All columns from the golden master are present in the output
    missing = set(golden_cols) - set(output_cols)
    assert not missing, f"Missing columns in output: {missing}"

    # Feature count (CSV records, not lines — handles multi-line quoted fields)
    assert len(output_rows) == len(
        golden_rows
    ), f"Feature count changed: output={len(output_rows)}, golden={len(golden_rows)}"

    # No completely empty rows
    assert all(
        any(v for v in row.values()) for row in output_rows
    ), "Output contains completely empty rows"

    wkt_col = golden_cols[0] if golden_cols and golden_cols[0] == "WKT" else None

    if wkt_col:
        # No empty geometries — allow only rows that were also empty in the golden
        golden_empty = {i for i, r in enumerate(golden_rows) if not r.get(wkt_col)}
        output_empty = {i for i, r in enumerate(output_rows) if not r.get(wkt_col)}
        unexpected_empty = output_empty - golden_empty
        assert not unexpected_empty, (
            f"Rows with unexpected empty WKT (not empty in golden): "
            f"{[i + 1 for i in sorted(unexpected_empty)[:5]]}"
        )

        # Geometry type unchanged (e.g. MULTIPOLYGON)
        golden_types = {
            r[wkt_col].split(" ((")[0] for r in golden_rows if r.get(wkt_col)
        }
        output_types = {
            r[wkt_col].split(" ((")[0] for r in output_rows if r.get(wkt_col)
        }
        assert (
            output_types == golden_types
        ), f"Geometry type changed: output={output_types}, golden={golden_types}"

        # Bounding box within tolerance — catches wrong projections or garbled geometry
        g_bbox = _bounding_box(golden_rows, wkt_col)
        o_bbox = _bounding_box(output_rows, wkt_col)
        if g_bbox and o_bbox:
            for i, label in enumerate(("min_x", "min_y", "max_x", "max_y")):
                assert abs(o_bbox[i] - g_bbox[i]) <= BBOX_TOLERANCE, (
                    f"Bounding box {label} drifted beyond tolerance: "
                    f"output={o_bbox[i]:.6f}, golden={g_bbox[i]:.6f}"
                )

    # Identifier columns: non-WKT columns where every golden value is unique and
    # non-empty. The set of values must match exactly between output and golden.
    for col in golden_cols:
        if col == wkt_col:
            continue
        golden_vals = [r[col] for r in golden_rows]
        if len(golden_vals) == len(set(golden_vals)) and all(golden_vals):
            output_vals = {r[col] for r in output_rows}
            assert (
                set(golden_vals) == output_vals
            ), f"Identifier column '{col}' values changed"


@pytest.mark.parametrize(
    "input_file",
    [
        "tests/data/resource_examples/csv.resource",
        "tests/data/resource_examples/xlsx.resource",
        "tests/data/resource_examples/xlsm.resource",
        "tests/data/resource_examples/geojson.resource",
        #  "tests/data/resource_examples/kml.resource", # failing
        "tests/data/resource_examples/kml_multilayer.resource",
        "tests/data/resource_examples/gml.resource",
        "tests/data/resource_examples/gml_very_long_rows.resource",
        "tests/data/resource_examples/shapefile_zip.resource",
        "tests/data/resource_examples/shapefile_zip_not_in_root.resource",
        "tests/data/resource_examples/geopackage.resource",
        "tests/data/resource_examples/utf-8-sig-json.resource",
    ],
)
def test_convert(input_file, tmp_path):
    input_file = pathlib.Path(input_file)
    output_file = tmp_path / (input_file.stem + ".csv")
    _execute_convert(input_file, output_file)
    golden_master = input_file.with_suffix(".csv")
    _assert_convert_properties(output_file, golden_master)


@pytest.fixture()
def input_file_xlsx(tmp_path):
    path = tmp_path / "input.xlsx"
    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet()
    worksheet.write("A1", "field-1")
    worksheet.write("B1", "field-2")
    worksheet.write("C1", "field-3")
    worksheet.write("A2", "row-1-data-1")
    worksheet.write("B2", "row-1-data-2")
    worksheet.write("C2", "row-1-data-3")
    worksheet.write("A3", "row-2-data-1")
    worksheet.write("B3", "row-2-data-2")
    worksheet.write("C3", "row-2-data-3")
    workbook.close()
    return path


@pytest.fixture()
def input_file_csv(tmp_path):
    p = tmp_path / "input.csv"
    fieldnames = ["field-1", "field-2", "field-3"]
    with open(p, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "field-1": "row-1-data-1",
                "field-2": "row-1-data-2",
                "field-3": "row-1-data-3",
            }
        )
        writer.writerow(
            {
                "field-1": "row-2-data-1",
                "field-2": "row-2-data-2",
                "field-3": "row-2-data-3",
            }
        )
    return p


def test_convert_xlsx(input_file_xlsx):
    _test_convert(input_file_xlsx)


def test_convert_csv(input_file_csv):
    _test_convert(input_file_csv)


def _test_convert(input_file):
    output_file = input_file.with_suffix(".out.csv")
    _execute_convert(input_file, output_file)
    output = read_csv(output_file)
    assert len(output) == 2
    assert output[0]["field-1"] == "row-1-data-1"
    assert output[1]["field-3"] == "row-2-data-3"


def _execute_convert(input_file, output_file):
    args = [
        "convert",
        str(input_file),
        str(output_file),
    ]
    runner = CliRunner()
    result = runner.invoke(cli, args)

    assert 0 == result.exit_code


def read_csv(file):
    with open(file) as f:
        csv_reader = csv.DictReader(f)
        return list(csv_reader)
