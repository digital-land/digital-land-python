import csv
import difflib
import filecmp
import pathlib

import pytest
import xlsxwriter
from tests.utils.helpers import execute, print_diffs



@pytest.mark.parametrize(
    "input_file",
    [
        "tests/data/resource_examples/csv.resource",
        "tests/data/resource_examples/xlsx.resource",
        "tests/data/resource_examples/xlsm.resource",
        "tests/data/resource_examples/geojson.resource",
        "tests/data/resource_examples/kml.resource",
        "tests/data/resource_examples/kml_multilayer.resource",
        "tests/data/resource_examples/gml.resource",
        "tests/data/resource_examples/gml_very_long_rows.resource",
        "tests/data/resource_examples/shapefile_zip.resource",
        "tests/data/resource_examples/shapefile_zip_not_in_root.resource",
        "tests/data/resource_examples/geopackage.resource",
    ],
)
def test_convert(input_file, tmp_path):
    input_file = pathlib.Path(input_file)
    output_file = tmp_path / (input_file.stem + ".csv")
    _execute_convert(input_file, output_file)
    golden_master = input_file.with_suffix(".csv")

    assert filecmp.cmp(output_file, golden_master), print_diffs(
        output_file, golden_master
    )


def diff_macos_fuzzy(fromfile, tofile):
    file_a = open(fromfile).readlines()
    file_b = open(tofile).readlines()
    matcher = difflib.SequenceMatcher(None, " ".join(file_a), " ".join(file_b))
    return matcher.quick_ratio()


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
    returncode, outs, errs = execute(
        [
            "digital-land",
            "-n",
            "some-pipeline",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "convert",
            input_file,
            output_file,
        ]
    )
    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs


def read_csv(file):
    with open(file) as f:
        csv_reader = csv.DictReader(f)
        return list(csv_reader)
