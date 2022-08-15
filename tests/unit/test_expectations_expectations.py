from digital_land.expectations.core import QueryRunner
from digital_land.expectations.expectations import (
    expect_database_to_have_set_of_tables,
    expect_table_to_have_set_of_columns,
    expect_table_row_count_to_be_in_range,
    expect_row_count_for_lookup_value_to_be_in_range,
    expect_field_values_to_be_within_set,
    expect_values_for_field_to_be_unique,
    expect_geoshapes_to_be_valid,
    expect_values_for_a_key_stored_in_json_are_within_a_set,
    expect_keys_in_json_field_to_be_in_set_of_options,
    expect_values_in_field_to_be_within_range,
    expect_custom_query_result_to_be_as_predicted,
)

# Shared testing resources
tested_dataset = (
    "tests/expectations/resources_to_test_expectations/lb_single_res.sqlite3"
)

query_runner = QueryRunner(tested_dataset)


def test_check_database_has_expected_tables_Success():
    "Test with tables that all exist in the database, with fail if found more than expected True"

    expected_table_set = {
        "column_field",
        "dataset_resource",
        "entity",
        "fact",
        "fact_resource",
        "issue",
        "old_entity",
    }

    response = expect_database_to_have_set_of_tables(
        query_runner, expected_table_set, fail_if_found_more_than_expected=True
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_database_has_expected_tables_Success_with_fme_false():
    "Test with tables that all exist in the database, with fail if found more than expected False"

    expected_table_set = {
        "column_field",
        "dataset_resource",
        "entity",
        "fact",
        "fact_resource",
        "issue",
        "old_entity",
    }

    response = expect_database_to_have_set_of_tables(
        query_runner, expected_table_set, fail_if_found_more_than_expected=False
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_database_has_expected_tables_Fail():
    "Test with one table that won't be found in the database, with fail if found more than expected True"

    expected_table_set = {
        "not_found_table",
        "column_field",
        "dataset_resource",
        "entity",
        "fact",
        "fact_resource",
        "issue",
        "old_entity",
    }
    response = expect_database_to_have_set_of_tables(
        query_runner, expected_table_set, fail_if_found_more_than_expected=True
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: difference between expected tables and found tables on the db see details"
    )
    assert response.details == {
        "expected_tables": {
            "dataset_resource",
            "fact",
            "old_entity",
            "not_found_table",
            "column_field",
            "issue",
            "entity",
            "fact_resource",
        },
        "found_tables": {
            "dataset_resource",
            "fact",
            "old_entity",
            "column_field",
            "issue",
            "entity",
            "fact_resource",
        },
    }


def test_check_database_has_expected_tables_Fail_with_fme_false():
    "Test with one table that won't be found in the database, with fail if found more than expected False"

    expected_table_set = {
        "not_found_table",
        "column_field",
        "dataset_resource",
        "entity",
        "fact",
        "fact_resource",
        "issue",
        "old_entity",
    }

    response = expect_database_to_have_set_of_tables(
        query_runner, expected_table_set, fail_if_found_more_than_expected=False
    )

    assert not response.result


def test_check_database_has_expected_tables_find_more_than_expected_fail():
    "All expected will be found, but will find one non-expected with fail if found more than expected True, should fail"

    expected_table_set = {
        "dataset_resource",
        "entity",
        "fact",
        "fact_resource",
        "issue",
        "old_entity",
    }

    response = expect_database_to_have_set_of_tables(
        query_runner, expected_table_set, fail_if_found_more_than_expected=True
    )

    assert not response.result


def test_check_table_has_expected_columns_Success():
    "Test with all columns found in the table, return should be True"

    table_name = "entity"
    expected_column_set = {
        "typology",
        "start_date",
        "reference",
        "geometry",
        "entity",
        "end_date",
        "json",
        "dataset",
        "geojson",
        "point",
        "entry_date",
        "name",
        "organisation_entity",
        "prefix",
    }

    response = expect_table_to_have_set_of_columns(
        query_runner, table_name=table_name, expected_columns_set=expected_column_set
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_table_has_expected_columns_Fail():
    "Test with a column that won't be found in the table, return should be False"

    table_name = "entity"
    expected_column_set = {
        "not_found_column",
        "typology",
        "start_date",
        "reference",
        "geometry",
        "entity",
        "end_date",
        "json",
        "dataset",
        "geojson",
        "point",
        "entry_date",
        "name",
        "organisation_entity",
        "prefix",
    }
    response = expect_table_to_have_set_of_columns(
        query_runner, table_name=table_name, expected_columns_set=expected_column_set
    )

    assert not response.result


def test_check_table_has_expected_columns_fail_if_find_more_than_expected_Fail():
    "All columns will be found, but there is a found column that was not expected 'typology', should return False"

    table_name = "entity"
    fail_if_found_more_than_expected = True
    expected_column_set = {
        "start_date",
        "reference",
        "geometry",
        "entity",
        "end_date",
        "json",
        "dataset",
        "geojson",
        "point",
        "entry_date",
        "name",
        "organisation_entity",
        "prefix",
    }
    response = expect_table_to_have_set_of_columns(
        query_runner,
        table_name=table_name,
        expected_columns_set=expected_column_set,
        fail_if_found_more_than_expected=fail_if_found_more_than_expected,
    )

    assert not response.result


def test_check_table_has_expected_columns_success_if_find_more_than_expected_Success():
    """All columns will be found, but there is a found column that was not expected 'typology',
    but flag to fail for this is off, should return True"""

    table_name = "entity"
    fail_if_found_more_than_expected = False
    expected_column_set = {
        "start_date",
        "reference",
        "geometry",
        "entity",
        "end_date",
        "json",
        "dataset",
        "geojson",
        "point",
        "entry_date",
        "name",
        "organisation_entity",
        "prefix",
    }
    response = expect_table_to_have_set_of_columns(
        query_runner,
        table_name=table_name,
        expected_columns_set=expected_column_set,
        fail_if_found_more_than_expected=fail_if_found_more_than_expected,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_row_count_of_table_is_in_expected_range_Success():
    "Test with count in the expected range"

    table_name = "entity"
    min_expected_row_count = 400
    max_expected_row_count = 500
    response = expect_table_row_count_to_be_in_range(
        query_runner=query_runner,
        table_name=table_name,
        min_expected_row_count=min_expected_row_count,
        max_expected_row_count=max_expected_row_count,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_row_count_of_table_is_in_expected_range_Fail():
    "Test with count not in the expected range"

    table_name = "entity"
    min_expected_row_count = 200
    max_expected_row_count = 300
    response = expect_table_row_count_to_be_in_range(
        query_runner=query_runner,
        table_name=table_name,
        min_expected_row_count=min_expected_row_count,
        max_expected_row_count=max_expected_row_count,
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: row count not in the expected range for table 'entity' see details"
    )
    assert response.details == {
        "table": "entity",
        "counted_rows": 200,
        "min_expected": 200,
        "max_expected": 300,
    }


def test_row_count_grouped_by_field_Success2():
    "Test with one of the count per value not within expected ranges"
    table_name = "fact"
    field_name = "entity"
    dict_value_and_count_ranges = [
        {"lookup_value": "42114488", "min_row_count": 8, "max_row_count": 10},
        {"lookup_value": "42114489", "min_row_count": 8, "max_row_count": 10},
        {"lookup_value": "42114490", "min_row_count": 8, "max_row_count": 10},
    ]

    response = expect_row_count_for_lookup_value_to_be_in_range(
        query_runner=query_runner,
        table_name=table_name,
        field_name=field_name,
        count_ranges_per_value=dict_value_and_count_ranges,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_row_count_grouped_by_field_Fail():
    "Test with one of the count per value not within expected range (42114490)"
    table_name = "fact"
    field_name = "entity"
    dict_value_and_count_ranges = [
        {"lookup_value": "42114488", "min_row_count": 8, "max_row_count": 10},
        {"lookup_value": "42114489", "min_row_count": 8, "max_row_count": 10},
        {"lookup_value": "42114490", "min_row_count": 6, "max_row_count": 8},
    ]

    response = expect_row_count_for_lookup_value_to_be_in_range(
        query_runner=query_runner,
        table_name=table_name,
        field_name=field_name,
        count_ranges_per_value=dict_value_and_count_ranges,
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: table 'fact': one or more counts per lookup_value not in expected range see for more info see details"
    )
    assert response.details == [
        {
            "lookup_value": "42114490",
            "min_row_count": 6,
            "max_row_count": 8,
            "rows_found": 9,
        }
    ]


def test_check_field_values_within_expected_set_of_values_No_unexpected_value():
    "Returns True as all values are within the expected set (but not full expected set is found)"
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = False
    expected_values_set = {
        "not_present_value",
        "entry-date",
        "organisation",
        "reference",
        "listed-building-grade",
        "geometry",
        "prefix",
        "notes",
        "name",
        "description",
    }

    response = expect_field_values_to_be_within_set(
        query_runner,
        table_name,
        field_name,
        expected_values_set,
        fail_if_not_found_entire_expected_set,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_field_values_within_expected_set_of_values_One_unexpected_value():
    "Returns False because it finds 'entry-date' as a value that was not in the the expected set"
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = False
    expected_values_set = {
        "not_present_value",
        "organisation",
        "reference",
        "listed-building-grade",
        "geometry",
        "prefix",
        "notes",
        "name",
        "description",
    }

    response = expect_field_values_to_be_within_set(
        query_runner,
        table_name,
        field_name,
        expected_values_set,
        fail_if_not_found_entire_expected_set,
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: values for field 'field' on table 'fact' do not fit expected set criteria, see details"
    )
    assert response.details == {
        "table": "fact",
        "expected_values": {
            "not_present_value",
            "name",
            "description",
            "organisation",
            "reference",
            "listed-building-grade",
            "prefix",
            "notes",
            "geometry",
        },
        "found_values": {
            "name",
            "entry-date",
            "description",
            "organisation",
            "reference",
            "listed-building-grade",
            "prefix",
            "notes",
            "geometry",
        },
    }


def test_check_field_values_within_expected_set_of_values_Not_found_entire_expected_set_found():
    """Returns False because even though all values are within expected set the flag
    fail_if_not_found_entire_expected_set is set to True and the 'not_present_value' is not
    found in the table field.
    """
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = True
    expected_values_set = {
        "not_present_value",
        "entry-date",
        "organisation",
        "reference",
        "listed-building-grade",
        "geometry",
        "prefix",
        "notes",
        "name",
        "description",
    }

    response = expect_field_values_to_be_within_set(
        query_runner,
        table_name,
        field_name,
        expected_values_set,
        fail_if_not_found_entire_expected_set,
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: values for field 'field' on table 'fact' do not fit expected set criteria, see details"
    )
    assert response.details == {
        "table": "fact",
        "expected_values": {
            "not_present_value",
            "description",
            "listed-building-grade",
            "reference",
            "geometry",
            "notes",
            "entry-date",
            "organisation",
            "prefix",
            "name",
        },
        "found_values": {
            "description",
            "listed-building-grade",
            "reference",
            "geometry",
            "notes",
            "entry-date",
            "organisation",
            "prefix",
            "name",
        },
    }


def test_check_field_values_within_expected_set_of_values_Entire_expected_set_found():
    """Returns True because all values are within expected and all are found and the flag
    fail_if_not_found_entire_expected_set is set to True.
    """
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = True
    expected_values_set = {
        "entry-date",
        "organisation",
        "reference",
        "listed-building-grade",
        "geometry",
        "prefix",
        "notes",
        "name",
        "description",
    }

    response = expect_field_values_to_be_within_set(
        query_runner,
        table_name,
        field_name,
        expected_values_set,
        fail_if_not_found_entire_expected_set,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_uniqueness_field_set_of_fields_True():
    """Test uniqueness with combination field that is unique.
    Should return True for uniqueness test
    """
    table_name = "fact"
    fields = ["fact"]

    response = expect_values_for_field_to_be_unique(query_runner, table_name, fields)

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_uniqueness_field_set_of_fields_False():
    """Test uniqueness with combination of 2 fields that have multiple values.
    Should return False for uniqueness test
    """
    table_name = "fact"
    fields = ["field", "entry_date"]

    response = expect_values_for_field_to_be_unique(query_runner, table_name, fields)

    assert not response.result
    assert (
        response.msg
        == "Fail: duplicate values for the combined fields '['field', 'entry_date']' on table 'fact', see details"
    )
    assert response.details == {
        "duplicates_found": [
            {
                "field": "description",
                "entry_date": "2022-07-31",
                "duplicates_count": 465,
            },
            {
                "field": "entry-date",
                "entry_date": "2022-07-31",
                "duplicates_count": 465,
            },
            {"field": "geometry", "entry_date": "2022-07-31", "duplicates_count": 489},
            {
                "field": "listed-building-grade",
                "entry_date": "2022-07-31",
                "duplicates_count": 465,
            },
            {"field": "name", "entry_date": "2022-07-31", "duplicates_count": 465},
            {"field": "notes", "entry_date": "2022-07-31", "duplicates_count": 465},
            {
                "field": "organisation",
                "entry_date": "2022-07-31",
                "duplicates_count": 465,
            },
            {"field": "prefix", "entry_date": "2022-07-31", "duplicates_count": 465},
            {"field": "reference", "entry_date": "2022-07-31", "duplicates_count": 465},
        ]
    }


def test_check_geo_shapes_are_valid_True():
    """Tests with five valid geo shapes, should return True."""
    tested_dataset = "tests/expectations/resources_to_test_expectations/five_valid_multipolygons.sqlite3"
    query_runner = QueryRunner(tested_dataset)

    table_name = "five_valid_multipolygons"
    shape_field = "geometry"
    ref_fields = ["entity"]

    response = expect_geoshapes_to_be_valid(
        query_runner, table_name, shape_field, ref_fields
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_geo_shapes_are_valid_False():
    """Tests with one geo shape that is invalid in a table with 5."""
    tested_dataset = "tests/expectations/resources_to_test_expectations/one_invalid_among_five.sqlite3"
    query_runner = QueryRunner(tested_dataset)

    table_name = "one_invalid_among_five"
    shape_field = "geometry"
    ref_fields = ["entity"]

    response = expect_geoshapes_to_be_valid(
        query_runner, table_name, shape_field, ref_fields
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: 1 invalid shapes found in field 'geometry' on table 'one_invalid_among_five', see details"
    )
    assert response.details == {"invalid_shapes": [{"entity": 303443, "is_valid": 0}]}


def test_check_json_values_for_key_within_expected_set_True():
    "Test case where all values found are within expected"
    table_name = "entity"
    field_name = "json"
    json_key = "listed-building-grade"
    ref_fields = ["entity"]

    expected_values_set = {"I", "II", "III", "II*"}

    response = expect_values_for_a_key_stored_in_json_are_within_a_set(
        query_runner, table_name, field_name, json_key, expected_values_set, ref_fields
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_json_values_for_key_within_expected_set_False():
    "Test case where II* is not expected but found"
    table_name = "entity"
    field_name = "json"
    json_key = "listed-building-grade"
    ref_fields = ["entity"]

    expected_values_set = {"I", "II", "III"}

    response = expect_values_for_a_key_stored_in_json_are_within_a_set(
        query_runner, table_name, field_name, json_key, expected_values_set, ref_fields
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: found non-expected values for key 'listed-building-grade' in field 'json' on table 'entity', see details"
    )
    assert response.details == {
        "non_expected_values": [
            {"entity": 42114490, "value_found_for_key": "II*"},
            {"entity": 42114499, "value_found_for_key": "II*"},
            {"entity": 42114503, "value_found_for_key": "II*"},
            {"entity": 42114516, "value_found_for_key": "II*"},
            {"entity": 42114519, "value_found_for_key": "II*"},
            {"entity": 42114521, "value_found_for_key": "II*"},
            {"entity": 42114530, "value_found_for_key": "II*"},
            {"entity": 42114537, "value_found_for_key": "II*"},
            {"entity": 42114552, "value_found_for_key": "II*"},
            {"entity": 42114582, "value_found_for_key": "II*"},
            {"entity": 42114583, "value_found_for_key": "II*"},
            {"entity": 42114599, "value_found_for_key": "II*"},
            {"entity": 42114605, "value_found_for_key": "II*"},
            {"entity": 42114612, "value_found_for_key": "II*"},
            {"entity": 42114644, "value_found_for_key": "II*"},
            {"entity": 42114645, "value_found_for_key": "II*"},
            {"entity": 42114646, "value_found_for_key": "II*"},
            {"entity": 42114648, "value_found_for_key": "II*"},
            {"entity": 42114652, "value_found_for_key": "II*"},
            {"entity": 42114653, "value_found_for_key": "II*"},
            {"entity": 42114655, "value_found_for_key": "II*"},
            {"entity": 42114669, "value_found_for_key": "II*"},
            {"entity": 42114676, "value_found_for_key": "II*"},
            {"entity": 42114677, "value_found_for_key": "II*"},
            {"entity": 42114678, "value_found_for_key": "II*"},
            {"entity": 42114688, "value_found_for_key": "II*"},
            {"entity": 42114699, "value_found_for_key": "II*"},
            {"entity": 42114707, "value_found_for_key": "II*"},
            {"entity": 42114711, "value_found_for_key": "II*"},
            {"entity": 42114712, "value_found_for_key": "II*"},
            {"entity": 42114722, "value_found_for_key": "II*"},
            {"entity": 42114725, "value_found_for_key": "II*"},
            {"entity": 42114742, "value_found_for_key": "II*"},
            {"entity": 42114776, "value_found_for_key": "II*"},
            {"entity": 42114790, "value_found_for_key": "II*"},
            {"entity": 42114802, "value_found_for_key": "II*"},
            {"entity": 42114824, "value_found_for_key": "II*"},
            {"entity": 42114843, "value_found_for_key": "II*"},
            {"entity": 42114854, "value_found_for_key": "II*"},
            {"entity": 42114856, "value_found_for_key": "II*"},
            {"entity": 42114869, "value_found_for_key": "II*"},
            {"entity": 42114871, "value_found_for_key": "II*"},
            {"entity": 42114880, "value_found_for_key": "II*"},
            {"entity": 42114881, "value_found_for_key": "II*"},
            {"entity": 42114883, "value_found_for_key": "II*"},
            {"entity": 42114913, "value_found_for_key": "II*"},
            {"entity": 42114917, "value_found_for_key": "II*"},
            {"entity": 42114922, "value_found_for_key": "II*"},
            {"entity": 42114940, "value_found_for_key": "II*"},
        ]
    }


def test_check_json_values_for__not_found_key_False():
    "Test case where all key is not found"
    table_name = "entity"
    field_name = "json"
    json_key = "not-present-key"
    ref_fields = ["entity"]

    expected_values_set = {"I", "II", "III", "II*"}

    response = expect_values_for_a_key_stored_in_json_are_within_a_set(
        query_runner, table_name, field_name, json_key, expected_values_set, ref_fields
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: found non-expected values for key 'not-present-key' in field 'json' on table 'entity', see details"
    )
    # because in this case the details will bring to many rows we decided not to assert details content


def test_check_json_keys_are_within_Expected_keys_set_True():
    "Test case where all keys found are within the expected set"
    table_name = "entity"
    field_name = "json"
    ref_fields = ["entity"]
    expected_key_set = {
        "listed-building-grade",
        "documentation-url",
        "description",
        "notes",
    }

    response = expect_keys_in_json_field_to_be_in_set_of_options(
        query_runner, table_name, field_name, expected_key_set, ref_fields
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_json_keys_are_within_Expected_keys_set_False():
    "Test case where a non-expected key is found, because we are not including 'notes' in the expected set"
    table_name = "entity"
    field_name = "json"
    ref_fields = ["entity"]
    expected_key_set = {"listed-building-grade", "documentation-url", "description"}

    response = expect_keys_in_json_field_to_be_in_set_of_options(
        query_runner, table_name, field_name, expected_key_set, ref_fields
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: found non-expected json keys in the field 'json' on table 'entity', see details"
    )
    # because in this case the details will bring ~450 rows we decided not to assert details content


def test_check_value_for_field_is_within_expected_range_True():
    "Test with field that have values inside the range"
    table_name = "entity"
    field_name = "reference"
    min_expected_value = 1021466
    max_expected_value = 1481085
    ref_fields = ["entity"]

    response = expect_values_in_field_to_be_within_range(
        query_runner,
        table_name,
        field_name,
        min_expected_value,
        max_expected_value,
        ref_fields,
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_value_for_field_is_within_expected_range_False():
    "Test with smaller max value for range, gets 18 records out of range, returns False"
    table_name = "entity"
    field_name = "reference"
    min_expected_value = 1021466
    max_expected_value = 1300000
    ref_fields = ["entity"]

    response = expect_values_in_field_to_be_within_range(
        query_runner,
        table_name,
        field_name,
        min_expected_value,
        max_expected_value,
        ref_fields,
    )

    assert not response.result
    assert (
        response.msg
        == "Fail: found 18 values out of the expected range for field 'reference' on table 'entity', see details"
    )
    assert response.details == {
        "records_with_value_out_of_range": [
            {"entity": 42114935, "reference": "1303676"},
            {"entity": 42114936, "reference": "1303751"},
            {"entity": 42114937, "reference": "1340606"},
            {"entity": 42114938, "reference": "1340607"},
            {"entity": 42114939, "reference": "1340608"},
            {"entity": 42114940, "reference": "1379929"},
            {"entity": 42114941, "reference": "1380619"},
            {"entity": 42114942, "reference": "1389103"},
            {"entity": 42114943, "reference": "1393287"},
            {"entity": 42114944, "reference": "1418993"},
            {"entity": 42114945, "reference": "1419345"},
            {"entity": 42114946, "reference": "1419396"},
            {"entity": 42114947, "reference": "1419405"},
            {"entity": 42114948, "reference": "1419823"},
            {"entity": 42114949, "reference": "1420087"},
            {"entity": 42114950, "reference": "1422933"},
            {"entity": 42114951, "reference": "1425089"},
            {"entity": 42114952, "reference": "1439997"},
        ]
    }


def test_check_custom_query_expectataion_True():
    "Test custom query with matching result"

    custom_query = "SELECT dataset, entity, typology, reference FROM entity WHERE reference IN (1090769,1090770,1090771,1090772)"
    expected_query_result = [
        {
            "dataset": "listed-building-outline",
            "entity": 42114488,
            "typology": "geography",
            "reference": "1090769",
        },
        {
            "dataset": "listed-building-outline",
            "entity": 42114489,
            "typology": "geography",
            "reference": "1090770",
        },
        {
            "dataset": "listed-building-outline",
            "entity": 42114490,
            "typology": "geography",
            "reference": "1090771",
        },
        {
            "dataset": "listed-building-outline",
            "entity": 42114491,
            "typology": "geography",
            "reference": "1090772",
        },
    ]

    response = expect_custom_query_result_to_be_as_predicted(
        query_runner, custom_query, expected_query_result
    )

    assert response.result
    assert response.msg == "Success: data quality as expected"


def test_check_custom_query_expectataion_Fail():
    "Test custom query with but receive one more row than expected in the result"

    custom_query = "SELECT dataset, entity, typology, reference FROM entity WHERE reference IN (1090769,1090770,1090771,1090772)"
    expected_query_result = [
        {
            "dataset": "listed-building-outline",
            "entity": 42114488,
            "typology": "geography",
            "reference": "1090769",
        },
        {
            "dataset": "listed-building-outline",
            "entity": 42114489,
            "typology": "geography",
            "reference": "1090770",
        },
        {
            "dataset": "listed-building-outline",
            "entity": 42114490,
            "typology": "geography",
            "reference": "1090771",
        },
    ]

    response = expect_custom_query_result_to_be_as_predicted(
        query_runner, custom_query, expected_query_result
    )

    print(response.details)

    assert not response.result
    assert (
        response.msg == "Fail: result for custom query was not as expected, see details"
    )
    assert response.details == {
        "custom_query": "SELECT dataset, entity, typology, reference FROM entity WHERE reference IN (1090769,1090770,1090771,1090772)",
        "query_result": [
            {
                "dataset": "listed-building-outline",
                "entity": 42114488,
                "typology": "geography",
                "reference": "1090769",
            },
            {
                "dataset": "listed-building-outline",
                "entity": 42114489,
                "typology": "geography",
                "reference": "1090770",
            },
            {
                "dataset": "listed-building-outline",
                "entity": 42114490,
                "typology": "geography",
                "reference": "1090771",
            },
            {
                "dataset": "listed-building-outline",
                "entity": 42114491,
                "typology": "geography",
                "reference": "1090772",
            },
        ],
        "expected_query_result": [
            {
                "dataset": "listed-building-outline",
                "entity": 42114488,
                "typology": "geography",
                "reference": "1090769",
            },
            {
                "dataset": "listed-building-outline",
                "entity": 42114489,
                "typology": "geography",
                "reference": "1090770",
            },
            {
                "dataset": "listed-building-outline",
                "entity": 42114490,
                "typology": "geography",
                "reference": "1090771",
            },
        ],
    }
