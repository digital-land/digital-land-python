import csv
import duckdb
import pytest

from digital_land.expectations.operations.csv import (
    count_rows,
    check_unique,
    check_no_shared_values,
    check_no_overlapping_ranges,
)


@pytest.fixture
def csv_file(tmp_path):
    file_path = tmp_path / "test.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "name", "reference"])
        writer.writerow(["1", "foo", "ref1"])
        writer.writerow(["2", "bar", "ref2"])
        writer.writerow(["3", "baz", "ref3"])
    return file_path


@pytest.fixture
def duckdb_conn():
    return duckdb.connect()


def test_count_rows_greater_than_passes(duckdb_conn, csv_file):
    passed, message, details = count_rows(duckdb_conn, file_path=csv_file, expected=2)
    assert passed is True
    assert details["actual"] == 3
    assert details["expected"] == 2


def test_count_rows_greater_than_fails(duckdb_conn, csv_file):
    passed, message, details = count_rows(duckdb_conn, file_path=csv_file, expected=5)
    assert passed is False
    assert details["actual"] == 3


def test_count_rows_equals_to(duckdb_conn, csv_file):
    passed, message, details = count_rows(
        duckdb_conn, file_path=csv_file, expected=3, comparison_rule="equals_to"
    )
    assert passed is True


def test_count_rows_equals_to_fails(duckdb_conn, csv_file):
    passed, message, details = count_rows(
        duckdb_conn, file_path=csv_file, expected=2, comparison_rule="equals_to"
    )
    assert passed is False


def test_count_rows_invalid_comparison_rule(duckdb_conn, csv_file):
    with pytest.raises(ValueError):
        count_rows(
            duckdb_conn, file_path=csv_file, expected=3, comparison_rule="invalid"
        )


def test_check_unique_passes(duckdb_conn, csv_file):
    passed, message, details = check_unique(
        duckdb_conn, file_path=csv_file, field="reference"
    )
    assert passed is True
    assert len(details["duplicates"]) == 0


@pytest.mark.parametrize(
    "rows",
    [
        [["a"], ["b"], ["a"]],
        [[1], [""], [1]],
    ],
)
def test_check_unique_fails(tmp_path, rows):
    file_path = tmp_path / "dupes.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name"])
        for row in rows:
            writer.writerow(row)

    conn = duckdb.connect()
    passed, message, details = check_unique(conn, file_path=file_path, field="name")
    assert passed is False
    assert len(details["duplicates"]) == 1
    assert details["duplicates"][0]["count"] == 2


@pytest.mark.parametrize(
    "rows",
    [
        [["a", "x"], ["b", "y"]],
        [["1", ""], ["2", "3"]],
    ],
)
def test_check_no_shared_values_passes(tmp_path, rows):
    file_path = tmp_path / "no_shared.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["col1", "col2"])
        for row in rows:
            writer.writerow(row)

    conn = duckdb.connect()
    passed, message, details = check_no_shared_values(
        conn, file_path=file_path, field_1="col1", field_2="col2"
    )
    assert passed is True
    assert len(details["shared_values"]) == 0


def test_check_no_shared_values_fails(tmp_path):
    file_path = tmp_path / "shared.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["col1", "col2"])
        writer.writerow(["a", "b"])
        writer.writerow(["b", "c"])

    conn = duckdb.connect()
    passed, message, details = check_no_shared_values(
        conn, file_path=file_path, field_1="col1", field_2="col2"
    )
    assert passed is False
    assert "b" in details["shared_values"]


def test_check_no_shared_values_ignores_empty(tmp_path):
    file_path = tmp_path / "empty_vals.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["col1", "col2"])
        writer.writerow(["a", ""])
        writer.writerow(["b", ""])

    conn = duckdb.connect()
    passed, message, details = check_no_shared_values(
        conn, file_path=file_path, field_1="col1", field_2="col2"
    )
    assert passed is True


@pytest.mark.parametrize(
    "rows",
    [
        [["1", "10"], ["11", "20"], ["21", "30"]],
        [["3000000000", "3000000010"], ["3000000011", "3000000020"]],  # BIGINT values
    ],
)
def test_check_no_overlapping_ranges_passes(tmp_path, rows):
    file_path = tmp_path / "ranges.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["min", "max"])
        for row in rows:
            writer.writerow(row)

    conn = duckdb.connect()
    passed, message, details = check_no_overlapping_ranges(
        conn, file_path=file_path, min_field="min", max_field="max"
    )
    assert passed is True
    assert len(details["overlaps"]) == 0


def test_check_no_overlapping_ranges_fails(tmp_path):
    file_path = tmp_path / "overlapping.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["min", "max"])
        writer.writerow(["1", "15"])
        writer.writerow(["10", "20"])

    conn = duckdb.connect()
    passed, message, details = check_no_overlapping_ranges(
        conn, file_path=file_path, min_field="min", max_field="max"
    )
    assert passed is False
    assert len(details["overlaps"]) == 1
    assert details["overlaps"][0]["range_1"] == ["1", "15"]
    assert details["overlaps"][0]["range_2"] == ["10", "20"]


def test_check_no_overlapping_ranges_adjacent_fails(tmp_path):
    """Adjacent ranges sharing a boundary value (e.g. [1,10] and [10,20]) are overlapping."""
    file_path = tmp_path / "adjacent.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["min", "max"])
        writer.writerow(["1", "10"])
        writer.writerow(["10", "20"])

    conn = duckdb.connect()
    passed, message, details = check_no_overlapping_ranges(
        conn, file_path=file_path, min_field="min", max_field="max"
    )
    assert passed is False
    assert len(details["overlaps"]) == 1
