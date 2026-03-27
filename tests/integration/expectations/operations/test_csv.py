import csv
import duckdb
import pytest

from digital_land.expectations.operations.csv import (
    count_rows,
    check_unique,
    check_no_shared_values,
    check_no_overlapping_ranges,
    check_allowed_values,
    check_no_blank_rows,
    check_fields_are_within_range,
    check_field_is_within_range_by_dataset_org,
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


def test_check_field_is_within_ranges_fails(tmp_path):
    lookup_file = tmp_path / "lookup.csv"
    with open(lookup_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity"])
        writer.writerow(["150"])
        writer.writerow(["999"])

    organisation_file = tmp_path / "entity-organisation.csv"
    with open(organisation_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])
        writer.writerow(["300", "400"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=lookup_file,
        external_file=organisation_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
    )

    assert passed is False
    assert "out-of-range" in message
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["value"] == 999


def test_check_field_is_within_ranges_ignores_org(tmp_path):
    lookup_file = tmp_path / "lookup.csv"
    with open(lookup_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity"])
        writer.writerow(["150"])
        writer.writerow(["250"])

    organisation_file = tmp_path / "entity-organisation.csv"
    with open(organisation_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])
        writer.writerow(["300", "400"])

    conn = duckdb.connect()
    # Test without match_fields - simple range check
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=lookup_file,
        external_file=organisation_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["value"] == 250


def test_check_allowed_values_fails_for_old_entity_status(tmp_path):
    file_path = tmp_path / "old-entity.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["old-entity", "status", "entity"])
        writer.writerow(["1001", "301", "2001"])
        writer.writerow(["1002", "410", "2002"])
        writer.writerow(["1003", "302", "2003"])

    conn = duckdb.connect()
    passed, message, details = check_allowed_values(
        conn,
        file_path=file_path,
        field="status",
        allowed_values=["301", "410"],
    )

    assert passed is False
    assert "invalid values" in message
    assert details["invalid_values"] == ["302"]
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["value"] == "302"


def test_check_allowed_values_passes_for_old_entity_status(tmp_path):
    file_path = tmp_path / "old-entity.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["old-entity", "status", "entity"])
        writer.writerow(["1001", "301", "2001"])
        writer.writerow(["1002", "410", "2002"])

    conn = duckdb.connect()
    passed, message, details = check_allowed_values(
        conn,
        file_path=file_path,
        field="status",
        allowed_values=["301", "410"],
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_no_blank_rows_passes(tmp_path):
    file_path = tmp_path / "no-blank-rows.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "name", "reference"])
        writer.writerow(["1", "foo", "ref1"])
        writer.writerow(["2", "bar", "ref2"])

    conn = duckdb.connect()
    passed, message, details = check_no_blank_rows(conn, file_path=file_path)

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_no_blank_rows_fails(tmp_path):
    file_path = tmp_path / "has-blank-rows.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "name", "reference"])
        writer.writerow(["1", "foo", "ref1"])
        writer.writerow(["", "", ""])
        writer.writerow([" ", "", "   "])
        writer.writerow(["2", "bar", "ref2"])

    conn = duckdb.connect()
    passed, message, details = check_no_blank_rows(conn, file_path=file_path)

    assert passed is False
    assert "blank rows" in message
    assert len(details["invalid_rows"]) == 2
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][1]["line_number"] == 4


def test_check_field_is_within_ranges_by_dataset_org_matches_prefix_and_organisation_fails(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "prefix", "organisation", "reference"])
        writer.writerow(["150", "dataset-a", "org-1", "ok-ref"])
        writer.writerow(["250", "dataset-a", "org-1", "bad-ref"])
        writer.writerow(["999", "dataset-a", "org-2", "other-org-ref"])

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["dataset", "organisation", "entity-minimum", "entity-maximum"])
        writer.writerow(["dataset-a", "org-1", "100", "200"])
        writer.writerow(["dataset-a", "org-2", "900", "1000"])

    conn = duckdb.connect()
    passed, message, details = check_field_is_within_range_by_dataset_org(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        lookup_dataset_field="prefix",
        range_dataset_field="dataset",
    )

    assert passed is False
    assert "out-of-range" in message
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["entity"] == 250
    assert details["invalid_rows"][0]["prefix"] == "dataset-a"
    assert details["invalid_rows"][0]["organisation"] == "org-1"


def test_check_field_is_within_ranges_by_dataset_org_matches_prefix_and_organisation_passes(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "prefix", "organisation", "reference"])
        writer.writerow(["150", "dataset-a", "org-1", "ok-ref"])
        writer.writerow(["950", "dataset-a", "org-2", "ok-ref-2"])

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["dataset", "organisation", "entity-minimum", "entity-maximum"])
        writer.writerow(["dataset-a", "org-1", "100", "200"])
        writer.writerow(["dataset-a", "org-2", "900", "1000"])

    conn = duckdb.connect()
    passed, message, details = check_field_is_within_range_by_dataset_org(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        lookup_dataset_field="prefix",
        range_dataset_field="dataset",
    )


def test_check_field_is_within_ranges_by_dataset_org_supports_custom_column_names(tmp_path):
    file_path = tmp_path / "lookup_custom.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity_value", "dataset_key", "organisation", "ref_code"])
        writer.writerow(["55", "dataset-x", "org-a", "ok-ref"])
        writer.writerow(["250", "dataset-x", "org-a", "bad-ref"])

    external_file = tmp_path / "ranges_custom.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["dataset_name", "organisation", "entity-minimum", "entity-maximum"])
        writer.writerow(["dataset-x", "org-a", "50", "100"])

    conn = duckdb.connect()
    passed, message, details = check_field_is_within_range_by_dataset_org(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity_value",
        lookup_dataset_field="dataset_key",
        range_dataset_field="dataset_name",
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["entity_value"] == 250
    assert details["invalid_rows"][0]["dataset_key"] == "dataset-x"
    assert details["invalid_rows"][0]["organisation"] == "org-a"


def test_check_field_is_within_ranges_filters_rows_with_lookup_rules(tmp_path):
    """Test filtering rows with lookup_rules during validation."""
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "status"])
        writer.writerow(["150", "active"])
        writer.writerow(["250", "active"])  
        writer.writerow(["350", "inactive"])

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        rules={"lookup_rules": {"status": "active"}},
    )
    assert passed is False
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["value"] == 250
    assert details["invalid_rows"][0]["line_number"] == 3


def test_check_field_is_within_ranges_lookup_rules_operator_eq_shape(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "prefix"])
        writer.writerow(["150", "conservationarea"]) 
        writer.writerow(["350", "other"]) 

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        rules={"lookup_rules": {"prefix": {"op": "=", "value": "conservationarea"}}},
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_field_is_within_ranges_lookup_rules_exact_match(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "prefix"])
        writer.writerow(["150", "conservationarea"])  
        writer.writerow(["350", "other"]) 

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        rules={"lookup_rules": {"prefix": "conservationarea"}},
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_field_is_within_ranges_lookup_rules_operator_in(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "organisation"])
        writer.writerow(["150", "org-a"])  
        writer.writerow(["350", "org-b"])  
        writer.writerow(["350", "org-c"])  

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity",
        rules={"lookup_rules": {"organisation": {"op": "in", "value": ["org-a", "org-b"]}}},
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["value"] == 350


def test_check_field_is_within_ranges_comma_separated_fields(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "end-entity"])
        writer.writerow(["150", "160"])  
        writer.writerow(["150", "350"])  
        writer.writerow(["350", "150"])  

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity, end-entity",
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2
    assert details["invalid_rows"][0]["line_number"] == 4
    assert details["invalid_rows"][0]["field"] == "entity"
    assert details["invalid_rows"][0]["value"] == 350
    assert details["invalid_rows"][1]["line_number"] == 3
    assert details["invalid_rows"][1]["field"] == "end-entity"
    assert details["invalid_rows"][1]["value"] == 350

def test_check_field_is_within_ranges_for_only_staus_301(tmp_path):
    file_path = tmp_path / "lookup.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "status","old-entity"])
        writer.writerow(["150", "301", "140"]) 
        writer.writerow(["250", "301", "150"]) 
        writer.writerow(["350", "410", "340"])

    external_file = tmp_path / "ranges.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity-minimum", "entity-maximum"])
        writer.writerow(["100", "200"])

    conn = duckdb.connect()
    passed, message, details = check_fields_are_within_range(
        conn,
        file_path=file_path,
        external_file=external_file,
        min_field="entity-minimum",
        max_field="entity-maximum",
        field="entity,old-entity",
        rules={"lookup_rules": {"status": {"op": "=", "value": "301"}}},
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 1
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["field"] == "entity"
    assert details["invalid_rows"][0]["value"] == 250
