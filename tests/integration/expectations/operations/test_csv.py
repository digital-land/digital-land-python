import csv
import duckdb
import pytest

from digital_land.expectations.operations.csv import (
    count_rows,
    check_unique,
    check_no_shared_values,
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
def duckdb_conn(csv_file):
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE csv_data AS SELECT * FROM read_csv_auto('{csv_file}')")
    return conn


def test_count_rows_greater_than_passes(duckdb_conn):
    passed, message, details = count_rows(duckdb_conn, expected=2)
    assert passed is True
    assert details["actual"] == 3
    assert details["expected"] == 2


def test_count_rows_greater_than_fails(duckdb_conn):
    passed, message, details = count_rows(duckdb_conn, expected=5)
    assert passed is False
    assert details["actual"] == 3


def test_count_rows_equals_to(duckdb_conn):
    passed, message, details = count_rows(
        duckdb_conn, expected=3, comparison_rule="equals_to"
    )
    assert passed is True


def test_count_rows_equals_to_fails(duckdb_conn):
    passed, message, details = count_rows(
        duckdb_conn, expected=2, comparison_rule="equals_to"
    )
    assert passed is False


def test_count_rows_invalid_comparison_rule(duckdb_conn):
    with pytest.raises(ValueError):
        count_rows(duckdb_conn, expected=3, comparison_rule="invalid")


def test_check_unique_passes(duckdb_conn):
    passed, message, details = check_unique(duckdb_conn, field="reference")
    assert passed is True
    assert len(details["duplicates"]) == 0


def test_check_unique_fails():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE csv_data AS SELECT * FROM (VALUES ('a'), ('b'), ('a')) AS t(name)"
    )
    passed, message, details = check_unique(conn, field="name")
    assert passed is False
    assert len(details["duplicates"]) == 1
    assert details["duplicates"][0]["value"] == "a"
    assert details["duplicates"][0]["count"] == 2


def test_check_no_shared_values_passes():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE csv_data AS SELECT * FROM (VALUES ('a', 'x'), ('b', 'y')) AS t(col1, col2)"
    )
    passed, message, details = check_no_shared_values(
        conn, field_1="col1", field_2="col2"
    )
    assert passed is True
    assert len(details["shared_values"]) == 0


def test_check_no_shared_values_fails():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE csv_data AS SELECT * FROM (VALUES ('a', 'b'), ('b', 'c')) AS t(col1, col2)"
    )
    passed, message, details = check_no_shared_values(
        conn, field_1="col1", field_2="col2"
    )
    assert passed is False
    assert "b" in details["shared_values"]


def test_check_no_shared_values_ignores_empty():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE csv_data AS SELECT * FROM (VALUES ('a', ''), ('b', '')) AS t(col1, col2)"
    )
    passed, message, details = check_no_shared_values(
        conn, field_1="col1", field_2="col2"
    )
    assert passed is True
