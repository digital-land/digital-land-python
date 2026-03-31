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
    check_values_have_the_correct_datatype,
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


def test_check_field_is_within_ranges_by_dataset_org_matches_prefix_and_organisation_fails(
    tmp_path,
):
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


def test_check_field_is_within_ranges_by_dataset_org_matches_prefix_and_organisation_passes(
    tmp_path,
):
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


def test_check_field_is_within_ranges_by_dataset_org_supports_custom_column_names(
    tmp_path,
):
    file_path = tmp_path / "lookup_custom.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity_value", "dataset_key", "organisation", "ref_code"])
        writer.writerow(["55", "dataset-x", "org-a", "ok-ref"])
        writer.writerow(["250", "dataset-x", "org-a", "bad-ref"])

    external_file = tmp_path / "ranges_custom.csv"
    with open(external_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["dataset_name", "organisation", "entity-minimum", "entity-maximum"]
        )
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
        rules={
            "lookup_rules": {"organisation": {"op": "in", "value": ["org-a", "org-b"]}}
        },
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
        writer.writerow(["entity", "status", "old-entity"])
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


def test_check_values_have_the_correct_datatype_passes(tmp_path):
    """Test datatype validation with all valid values."""
    file_path = tmp_path / "valid_datatypes.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "count", "enabled"])
        writer.writerow(["entity-1", "100", "true"])
        writer.writerow(["entity-2", "200", "false"])

    field_datatype = {
        "entity": "reference",
        "count": "integer",
        "enabled": "flag",
    }

    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_values_have_the_correct_datatype_fails(tmp_path):
    """Test datatype validation with invalid values."""
    file_path = tmp_path / "invalid_datatypes.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "count", "enabled"])
        writer.writerow(["entity-1", "100", "true"])
        writer.writerow(["entity-2", "not_a_number", "false"])
        writer.writerow(["entity-3", "300", "maybe"])

    field_datatype = {
        "entity": "reference",
        "count": "integer",
        "enabled": "flag",
    }

    conn = duckdb.connect()

    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2
    assert details["invalid_rows"][0]["line_number"] == 3
    assert details["invalid_rows"][0]["field"] == "count"
    assert details["invalid_rows"][0]["value"] == "not_a_number"
    assert details["invalid_rows"][0]["datatype"] == "integer"
    assert details["invalid_rows"][1]["line_number"] == 4
    assert details["invalid_rows"][1]["field"] == "enabled"
    assert details["invalid_rows"][1]["value"] == "maybe"
    assert "invalid datatype value(s)" in message


def test_check_values_have_the_correct_datatype_ignores_empty_values(tmp_path):
    """Test that empty values are skipped during validation."""
    file_path = tmp_path / "with_empty_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "count"])
        writer.writerow(["entity-1", "100"])
        writer.writerow(["entity-2", ""])
        writer.writerow(["entity-3", "300"])

    field_datatype = {
        "entity": "reference",
        "count": "integer",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_values_have_the_correct_datatype_skips_unmapped_fields(tmp_path):
    """Test that fields not in field_datatype map are not validated."""
    file_path = tmp_path / "unmapped_fields.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "count", "description"])
        writer.writerow(["entity-1", "100", "invalid_but_ignored"])

    field_datatype = {
        "entity": "reference",
        "count": "integer",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_values_have_the_correct_datatype_empty_file(tmp_path):
    """Test behavior with empty CSV file."""
    file_path = tmp_path / "empty.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "count"])

    field_datatype = {
        "entity": "reference",
        "count": "integer",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_values_have_the_correct_datatype_no_applicable_fields(tmp_path):
    """Test when no fields have datatype validators."""
    file_path = tmp_path / "no_applicable.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "description"])
        writer.writerow(["field1", "some value"])

    field_datatype = {
        "name": "string",
        "description": "string",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is True
    assert details["invalid_rows"] == []


def test_check_values_have_the_correct_datatype_decimal(tmp_path):
    """Test decimal datatype validation with both valid and invalid values."""
    file_path = tmp_path / "decimal_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["price"])
        writer.writerow(["100.50"])
        writer.writerow(["0.99"])
        writer.writerow(["999.999"])
        writer.writerow(["not-a-decimal"])
        writer.writerow(["12abc"])

    field_datatype = {"price": "decimal"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2
    assert any(r["value"] == "not-a-decimal" for r in details["invalid_rows"])
    assert any(r["value"] == "12abc" for r in details["invalid_rows"])


def test_check_values_have_the_correct_datatype_latitude_longitude(tmp_path):
    """Test latitude and longitude datatype validation with valid and invalid values."""
    file_path = tmp_path / "coordinates.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["latitude", "longitude"])
        writer.writerow(["0", "0"])
        writer.writerow(["51.5074", "-0.1278"])
        writer.writerow(["-33.8688", "151.2093"])
        writer.writerow(["90", "180"])
        writer.writerow(["91", "0"])
        writer.writerow(["0", "181"])

    field_datatype = {
        "latitude": "latitude",
        "longitude": "longitude",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_flag(tmp_path):
    """Test flag datatype validation with valid and invalid values."""
    file_path = tmp_path / "flag_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["active"])
        writer.writerow(["true"])
        writer.writerow(["false"])
        writer.writerow(["y"])
        writer.writerow(["n"])
        writer.writerow(["yes"])
        writer.writerow(["no"])
        writer.writerow(["maybe"])
        writer.writerow(["1"])

    field_datatype = {"active": "flag"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 4


def test_check_values_have_the_correct_datatype_hash(tmp_path):
    """Test hash datatype validation with valid and invalid values."""
    file_path = tmp_path / "hash_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["content_hash"])
        writer.writerow(["abcdef123456"])
        writer.writerow(["abc:1234567890abcdef"])
        writer.writerow(["sha:5d41402abc4b2a76b9719d911017c592"])
        writer.writerow(["not-a-hash"])
        writer.writerow(["xyz:notahex"])

    field_datatype = {"content_hash": "hash"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_curie(tmp_path):
    """Test curie datatype validation with valid and invalid values."""
    file_path = tmp_path / "curie_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["identifier"])
        writer.writerow(["prefix:value"])
        writer.writerow(["org:entity123"])
        writer.writerow(["schema:name"])
        writer.writerow(["prefix:"])
        writer.writerow(["no_colon"])
        writer.writerow(["prefix: space"])

    field_datatype = {"identifier": "curie"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 3


def test_check_values_have_the_correct_datatype_curie_list(tmp_path):
    """Test curie-list datatype validation with valid and invalid values."""
    file_path = tmp_path / "curie_list_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["identifiers"])
        writer.writerow(["prefix:value1;org:value2"])
        writer.writerow(["schema:name"])
        writer.writerow([""])
        writer.writerow(["not-valid"])
        writer.writerow(["prefix: value"])

    field_datatype = {"identifiers": "curie-list"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_json(tmp_path):
    """Test json datatype validation with valid and invalid JSON."""
    file_path = tmp_path / "json_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["data"])
        writer.writerow(['{"key":"value"}'])
        writer.writerow(['{"nested":{"field":"value"}}'])
        writer.writerow(["not json"])  # Invalid
        writer.writerow(['{"incomplete":'])  # Invalid (malformed)

    field_datatype = {"data": "json"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_url(tmp_path):
    """Test url datatype validation with valid and invalid URLs."""
    file_path = tmp_path / "url_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["website"])
        writer.writerow(["https://example.com"])
        writer.writerow(["http://test.org"])
        writer.writerow(["ftp://files.example.com"])
        writer.writerow(["not a url"])  # Invalid (no scheme)
        writer.writerow(["example.com"])  # Invalid (no scheme)

    field_datatype = {"website": "url"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_date(tmp_path):
    """Test date datatype validation with valid and invalid dates."""
    file_path = tmp_path / "date_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["start_date"])
        writer.writerow(["2024-01-15"])
        writer.writerow(["2023-12-31"])
        writer.writerow(["2022-06-30"])
        writer.writerow(["not-a-date"])
        writer.writerow(["2024-13-01"])

    field_datatype = {"start_date": "date"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_datetime(tmp_path):
    """Test datetime datatype validation with valid and invalid datetimes."""
    file_path = tmp_path / "datetime_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp"])
        writer.writerow(["2024-01-15T10:30:45"])
        writer.writerow(["2023-12-31T23:59:59Z"])
        writer.writerow(["2022-06-30T12:00:00+00:00"])
        writer.writerow(["not-a-datetime"])
        writer.writerow(["2024-13-01T10:00:00"])

    field_datatype = {"timestamp": "datetime"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_pattern(tmp_path):
    """Test pattern datatype validation with valid and invalid regex patterns."""
    file_path = tmp_path / "pattern_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["regex"])
        writer.writerow(["^[A-Z]+$"])
        writer.writerow(["\\d{3}-\\d{4}"])
        writer.writerow(["(foo|bar)"])
        writer.writerow(["["])
        writer.writerow(["(unclosed"])

    field_datatype = {"regex": "pattern"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_point(tmp_path):
    """Test point datatype validation (WKT format) with valid and invalid values."""
    file_path = tmp_path / "point_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["geometry"])
        writer.writerow(["POINT(0 0)"])
        writer.writerow(["POINT(51.5074 -0.1278)"])
        writer.writerow(["POINT(-33.8688 151.2093)"])
        writer.writerow(["not wkt"])
        writer.writerow(["POINT(0)"])

    field_datatype = {"geometry": "point"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_multipolygon(tmp_path):
    """Test multipolygon datatype validation (WKT format) with valid and invalid values."""
    file_path = tmp_path / "multipolygon_values.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["boundary"])
        writer.writerow(["POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"])
        writer.writerow(
            [
                "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))"
            ]
        )
        writer.writerow(["not wkt"])  # Invalid
        writer.writerow(["POINT(0 0)"])  # Invalid (not a polygon/multipolygon)

    field_datatype = {"boundary": "multipolygon"}
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 2


def test_check_values_have_the_correct_datatype_mixed_types(tmp_path):
    """Test validation with multiple different datatypes in one file."""
    file_path = tmp_path / "mixed_datatypes.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "price", "active", "latitude", "url", "date"])
        writer.writerow(
            ["org-001", "99.99", "true", "51.5074", "https://example.com", "2024-01-15"]
        )
        writer.writerow(
            ["org-002", "150.50", "false", "-33.8688", "https://test.org", "2023-12-31"]
        )
        writer.writerow(
            ["org 003", "invalid", "maybe", "91", "not-a-url", "not-a-date"]
        )

    field_datatype = {
        "price": "decimal",
        "active": "flag",
        "latitude": "latitude",
        "url": "url",
        "date": "date",
    }
    conn = duckdb.connect()
    passed, message, details = check_values_have_the_correct_datatype(
        conn, file_path, field_datatype
    )

    assert passed is False
    assert len(details["invalid_rows"]) == 5
