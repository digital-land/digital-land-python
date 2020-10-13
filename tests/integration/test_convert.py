import csv
import difflib
import filecmp
import pathlib
import platform

import pytest
import xlsxwriter
from helpers import execute
from wasabi import color


running_on_macos = platform.system() == "Darwin"


@pytest.mark.parametrize(
    "input_file, fuzzy_macos",
    [
        ("tests/data/resource_examples/csv.resource", False),
        ("tests/data/resource_examples/xlsx.resource", False),
        ("tests/data/resource_examples/xlsm.resource", False),
        ("tests/data/resource_examples/geojson.resource", False),
        ("tests/data/resource_examples/kml.resource", False),
        ("tests/data/resource_examples/kml_multilayer.resource", False),
        ("tests/data/resource_examples/gml.resource", False),
        ("tests/data/resource_examples/gml_very_long_rows.resource", False),
        ("tests/data/resource_examples/shapefile_zip.resource", True),
        ("tests/data/resource_examples/shapefile_zip_not_in_root.resource", True),
    ],
)
def test_convert(input_file, fuzzy_macos, tmp_path):
    input_file = pathlib.Path(input_file)
    output_file = tmp_path / (input_file.stem + ".csv")
    _execute_convert(input_file, output_file)
    golden_master = input_file.with_suffix(".csv")

    # This is due to annoying differences between the output of the ogr2ogr tool
    # on MacOS vs Linux, due to differences in floating point arithmetic.
    # If we are on mac, just assert that the files are similar enough, rather
    # than identical as we do for CI tests.
    if running_on_macos and fuzzy_macos:
        print("performing fuzzy match because macos")
        diff_ratio = diff_macos_fuzzy(output_file, golden_master)
        assert diff_ratio > 0.99, "macos fuzzy match not strong enough"
    else:
        assert filecmp.cmp(output_file, golden_master), print_diffs(
            output_file, golden_master
        )


def diff_macos_fuzzy(fromfile, tofile):
    file_a = open(fromfile).readlines()
    file_b = open(tofile).readlines()
    matcher = difflib.SequenceMatcher(None, " ".join(file_a), " ".join(file_b))
    return matcher.quick_ratio()


def print_diffs(fromfile, tofile):
    # helper function to print detailed diffs between two files
    file_a = open(fromfile).readlines()
    file_b = open(tofile).readlines()
    count = 0
    message = []
    for a, b in zip(file_a, file_b):
        count += 1
        if a == b:
            continue
        message.append(f"line {count} differs: ")
        matcher = difflib.SequenceMatcher(None, a, b)
        output = []
        for opcode, a0, a1, b0, b1 in matcher.get_opcodes():
            if opcode == "equal":
                output.append(a[a0:a1])
            elif opcode == "insert":
                output.append(color(b[b0:b1], fg=16, bg="green"))
            elif opcode == "delete":
                output.append(color(a[a0:a1], fg=16, bg="red"))
            elif opcode == "replace":
                output.append(color(b[b0:b1], fg=16, bg="green"))
                output.append(color(a[a0:a1], fg=16, bg="red"))
        message.append("".join(output))
    return "".join(message)


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
