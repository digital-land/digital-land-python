import inspect
import pandas as pd
from .core import QueryRunner, ExpectationResponse
from math import inf


def expect_database_to_have_set_of_tables(
    query_runner: QueryRunner,
    expected_tables_set: set,
    fail_if_found_more_than_expected: bool = False,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a set with table names and checks if all of the tables
    can be found in the database. It returns True if all found and False
    if at least one is not found. By default it is one-sided, if you with
    to fail in cases where found more tables than expected change optional
    argument fail_if_found_more_than_expected to True
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()
    expected_tables_set = set(expected_tables_set)

    sql_query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    found_tables_set = query_runner.run_query(
        sql_query, return_only_first_col_as_set=True
    )

    if fail_if_found_more_than_expected:
        result = expected_tables_set == found_tables_set
    else:
        result = expected_tables_set.issubset(found_tables_set)

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: difference between expected tables and found tables on the db see details"
        details = {
            "expected_tables": expected_tables_set,
            "found_tables": found_tables_set,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_table_to_have_set_of_columns(
    query_runner: QueryRunner,
    table_name: str,
    expected_columns_set: set,
    fail_if_found_more_than_expected: bool = False,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name and a set with column names and checks if all of
    columns can be found in the table. It returns True if all found and False
    if at least one is not found. It doesn't verify if additional columns are
    present in the table.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()
    expected_columns_set = set(expected_columns_set)

    sql_query = f"SELECT name FROM pragma_table_info('{table_name}');"
    found_columns_set = query_runner.run_query(
        sql_query, return_only_first_col_as_set=True
    )

    if fail_if_found_more_than_expected:
        result = expected_columns_set == found_columns_set
    else:
        result = expected_columns_set.issubset(found_columns_set)

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: difference between expected columns and found columnson table '{table_name}' see details"
        details = {
            "table": table_name,
            "expected_columns": expected_columns_set,
            "found_columns": found_columns_set,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_table_row_count_to_be_in_range(
    query_runner: QueryRunner,
    table_name: str,
    min_expected_row_count: int = 0,
    max_expected_row_count: int = inf,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name and a min and max for row count. It returns True
    if the row count is within the range and False otherwise, inclusive of min
    and max.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    sql_query = f"SELECT COUNT(*) AS row_count FROM {table_name};"
    counted_rows = query_runner.run_query(sql_query)["row_count"][0]

    result = min_expected_row_count <= counted_rows <= max_expected_row_count

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: row count not in the expected range for table '{table_name}' see details"
        details = {
            "table": table_name,
            "counted_rows": min_expected_row_count,
            "min_expected": min_expected_row_count,
            "max_expected": max_expected_row_count,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input, result=result, msg=msg, details=details
    )

    return expectation_response


def expect_row_count_for_lookup_value_to_be_in_range(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    count_ranges_per_value: dict,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a field name and a dictionary with row count
    ranges for how many rows have the values for that field. For example:
    with a table Person, a field Mother_First_Name and a dictionary:
        {"mary":(2,5), "ellen": (1,3)}
    it will test if the number of rows in the table Person that have the
    field Mother_First_Name = "mary" is between 2 and 5 (inclusive of 2 and 5)
    and if the number of rows that have Mother_First_Name = "ellen" is between
    1 and 3. If the row counts for all fields are inside the ranges it will
    return True, if at least one of the counts is not inside the range it will
    return False
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    df_expected_counts_by_value = pd.DataFrame(count_ranges_per_value)
    df_expected_counts_by_value["lookup_value"] = df_expected_counts_by_value[
        "lookup_value"
    ].astype("string")

    sql_query = f"SELECT {field_name} AS lookup_value,COUNT(*) AS rows_found FROM {table_name} GROUP BY {field_name};"
    counted_rows_by_grouped_value = query_runner.run_query(sql_query)
    counted_rows_by_grouped_value["lookup_value"] = counted_rows_by_grouped_value[
        "lookup_value"
    ].astype("string")

    expected_versus_found_df = df_expected_counts_by_value.merge(
        counted_rows_by_grouped_value, on="lookup_value", how="left"
    )

    found_not_within_range = expected_versus_found_df.loc[
        (
            expected_versus_found_df["rows_found"]
            >= expected_versus_found_df["max_row_count"]
        )
        | (
            expected_versus_found_df["rows_found"]
            <= expected_versus_found_df["min_row_count"]
        )
    ]

    result = len(found_not_within_range) == 0

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: table '{table_name}': one or more counts per lookup_value not in expected range see for more info see details"
        details = found_not_within_range.to_dict(orient="records")

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_field_values_to_be_within_set(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    expected_values_set: set,
    fail_if_not_found_entire_expected_set: bool = False,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a field and a set expected values and
    checks if values found for that field are within the expected set
    It returns True if all are in the expected set and False if at
    least one is not.
    By default is single-sided: returns False only if found a value that
    is not in the expected set, but doens't return False if an non-expected
    value is present.
    If the flag fail_if_not_found_entire_expected_set is set to True it will
    also return False in cases where a value of the expected set was not
    present in the table.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()
    expected_values_set = set(expected_values_set)

    sql_query = f"SELECT {field_name} FROM {table_name} GROUP BY 1;"
    found_values_set = query_runner.run_query(
        sql_query, return_only_first_col_as_set=True
    )

    if fail_if_not_found_entire_expected_set:
        result = expected_values_set == found_values_set
    else:
        result = found_values_set.issubset(expected_values_set)

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: values for field '{field_name}' on table '{table_name}' do not fit expected set criteria, see details"
        details = {
            "table": table_name,
            "expected_values": expected_values_set,
            "found_values": found_values_set,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_values_for_field_to_be_unique(
    query_runner: QueryRunner,
    table_name: str,
    fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a field (or a set of fields) and checks
    the table doesn't have duplicity for that field (or set of fields).
    Returns True if in the table there are not 2 rows with identical values
    for the field (or set of fields) and False otherwise.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    str_fields = ",".join(fields)
    sql_query = f"SELECT {str_fields},COUNT(*) AS duplicates_count FROM {table_name} GROUP BY {str_fields} HAVING COUNT(*)>1;"

    found_duplicity = query_runner.run_query(sql_query)

    result = len(found_duplicity) == 0

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: duplicate values for the combined fields '{fields}' on table '{table_name}', see details"
        details = {"duplicates_found": found_duplicity.to_dict(orient="records")}

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_geoshapes_to_be_valid(
    query_runner: QueryRunner,
    table_name: str,
    shape_field: str,
    ref_fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a shape field and an shape ref field (or set of)
    checks that the shapes are valid. Returns True if all are valid. Returns
    False if shapes are invalid and in the details returns ref for the shapes
    that were invalid (is_valid=0) or if shape parsing failed unknown(is_valid=-1).
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    str_ref_fields = ",".join(ref_fields)
    sql_query = f"""
        SELECT {str_ref_fields}, ST_IsValid(ST_GeomFromText({shape_field})) AS is_valid
        FROM {table_name}
        WHERE ST_IsValid(ST_GeomFromText({shape_field})) IN (0,-1);"""
    invalid_shapes = query_runner.run_query(sql_query)

    result = len(invalid_shapes) == 0

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: {len(invalid_shapes)} invalid shapes found in field '{shape_field}' on table '{table_name}', see details"
        details = {"invalid_shapes": invalid_shapes.to_dict(orient="records")}

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_values_for_a_key_stored_in_json_are_within_a_set(
    query_runner: QueryRunner,
    table_name: str,
    field: str,
    json_key: str,
    expected_values_set: set,
    ref_fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives:
        - table name,
        - a field (with a JSON text in it),
        - a json key (that for which the value will be looked)
        - one or more ref columns in a list,
        - a set expected values.
    It returns True if both conditions are met:
        - the key was found for all rows
        - the value stored for the key was within expected set
    If any is not met it returns False.
    One-sided: will not check if all expected values are found, only if all
    found values are within the expected
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()
    expected_values_set = set(expected_values_set)

    str_ref_fields = ",".join(ref_fields)
    str_expected_values_set = "','".join(expected_values_set)

    sql_query = f"""
        SELECT {str_ref_fields},json_extract({field}, '$.{json_key}') AS value_found_for_key
        FROM {table_name}
        WHERE
            (json_extract({field}, '$.{json_key}') NOT IN ('{str_expected_values_set}'))
            OR (json_extract({field}, '$.{json_key}')) IS NULL;"""

    non_expected_values = query_runner.run_query(sql_query)

    result = len(non_expected_values) == 0

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: found non-expected values for key '{json_key}' in field '{field}' on table '{table_name}', see details"
        details = {"non_expected_values": non_expected_values.to_dict(orient="records")}

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_keys_in_json_field_to_be_in_set_of_options(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    expected_keys_set: set,
    ref_fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a field name (of a field that has a JSON text
    stored in it) and a set of keys. It checks if the keys found in the JSON
    are within the expected set of expected keys.
    One sided: will not check if all expected keys are found, only if all
    found are within the expected
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()
    expected_keys_set = set(expected_keys_set)

    str_ref_fields = ",".join(ref_fields)

    prep_key_set = [f"'$.{s}'" for s in expected_keys_set]
    str_expected_keys_set = ",".join(prep_key_set)

    sql_query = (
        f"""
        SELECT {str_ref_fields},json_remove({field_name}, {str_expected_keys_set}) AS non_expected_keys
        FROM {table_name}
        WHERE non_expected_keys IS NOT NULL AND non_expected_keys <> '"""
        + "{}'"
    )

    non_expected_keys = query_runner.run_query(sql_query)

    result = len(non_expected_keys) == 0
    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: found non-expected json keys in the field '{field_name}' on table '{table_name}', see details"
        details = {
            "records_with_non_expected_keys": non_expected_keys.to_dict(
                orient="records"
            )
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_values_in_field_to_be_within_range(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    min_expected_value: int,
    max_expected_value: int,
    ref_fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a table name, a field name checks the values found in the field
    are within the expected range
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    str_ref_fields = ",".join(ref_fields)

    sql_query = f"""SELECT {str_ref_fields},{field_name}
                    FROM {table_name}
                    WHERE {field_name} < {min_expected_value} OR {field_name} > {max_expected_value}"""

    records_with_value_out_of_range = query_runner.run_query(sql_query)

    result = len(records_with_value_out_of_range) == 0
    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = f"Fail: found {len(records_with_value_out_of_range)} values out of the expected range for field '{field_name}' on table '{table_name}', see details"
        details = {
            "records_with_value_out_of_range": records_with_value_out_of_range.to_dict(
                orient="records"
            )
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_custom_query_result_to_be_as_predicted(
    query_runner: QueryRunner,
    custom_query: str,
    expected_query_result: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Receives a custom sqlite/spatialite query as string and a expected
    result in the form of list of row dictionaires, for example, 3 rows
    would look like this:

    [{'col_name1': value1, 'col_name_2': value2, 'col_name_3': value3},
     {'col_name1': value1, 'col_name_2': value2, 'col_name_3': value3},
     {'col_name1': value1, 'col_name_2': value2, 'col_name_3': value3}]

    Returns True if the result for the query are as expected and False
    otherwise, with details.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    query_result = query_runner.run_query(custom_query)

    result = query_result.to_dict(orient="records") == expected_query_result

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result for custom query was not as expected, see details"
        details = {
            "custom_query": custom_query,
            "query_result": query_result.to_dict(orient="records"),
            "expected_query_result": expected_query_result,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_urls_stored_for_a_key_in_json_to_end_in_expected_domain_endings(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    json_key: str,
    list_of_domain_endings: list,
    ref_fields: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    """Looks at urls stored inside a json field within a given json_key:
        - returns FALSE if the value doesn't start with "http://" or "https://"
        - checks if the domain ends with one of the values provided in the
        list_of_domain_endings, returns:
            - FALSE if it finds one or more records whose endings are not in
            the list
            - TRUE if all domain endings are in the list provided

    NOTE: The domain here is all that was found between "http://" (or https://)
     and the first "/" after it.
    """
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    str_ref_fields = ",".join(ref_fields)

    sql_look_for_inv_urls = f"""
                    WITH prepare as
                        (SELECT
                            {str_ref_fields},
                            json_extract({field_name}, '$.{json_key}') as url
                        FROM {table_name}
                        WHERE json_extract(json, '$.{json_key}') IS NOT NULL)

                    SELECT {str_ref_fields}, url
                    FROM prepare
                    WHERE INSTR(url, 'https://') = 0 AND INSTR(url, 'http://') = 0
                    """

    found_invalid_urls = query_runner.run_query(sql_look_for_inv_urls)

    if len(found_invalid_urls) != 0:
        # if doesn't start with http:// or https:// it stops early
        result = False
        msg = f"Fail: found {len(found_invalid_urls)} invalid URLs, see details"
        details = {"invalid_urls_found": found_invalid_urls.to_dict(orient="records")}
    else:
        sql_check_domain = f"""
            WITH prepare as
                (SELECT
                    {str_ref_fields},
                    json_extract({field_name}, '$.{json_key}') as url
                FROM {table_name}
                WHERE json_extract(json, '$.{json_key}') IS NOT NULL)
            SELECT
                {str_ref_fields},
                SUBSTR(
                    SUBSTR(url, INSTR(url, '//') + 2),
                    0,
                    INSTR(SUBSTR(url, INSTR(url, '//') + 2),'/'))
                AS extracted_domain
            FROM prepare
            WHERE extracted_domain NOT LIKE '%{list_of_domain_endings[0]}'
        """
        for ending in list_of_domain_endings[1:]:
            sql_check_domain = (
                sql_check_domain + "    AND extracted_domain NOT LIKE '%" + ending + "'"
            )

        found_unexpected_domain = query_runner.run_query(sql_check_domain)

        result = len(found_unexpected_domain) == 0

        if result:
            msg = "Success: data quality as expected"
            details = None
        else:
            msg = f"Fail: found {len(found_unexpected_domain)} records with unexpected domains, see details"
            details = {
                "records_with_unexpected_domains": found_unexpected_domain.to_dict(
                    orient="records"
                )
            }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_entities_to_intersect_given_geometry_to_be_as_predicted(
    query_runner: QueryRunner,
    geometry: str,
    expected_query_result: list,
    entity_geometry_field: str = "geometry",
    returned_entity_fields: list = ["name"],
    returned_json_fields: list = None,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    if returned_json_fields is not None:
        json_sql_list = [
            f"json_extract(json,'$.{field}') as '{field}'"
            for field in returned_json_fields
        ]
        fields = [*returned_entity_fields, *json_sql_list]
    else:
        fields = returned_entity_fields

    custom_query = f"SELECT {','.join(fields)} FROM entity WHERE ST_Intersects(GeomFromText({entity_geometry_field}),GeomFromText('{geometry}'));"

    query_result = query_runner.run_query(custom_query)

    result = query_result.to_dict(orient="records") == expected_query_result

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result for lasso query was not as expected, see details"
        details = {
            "query_result": query_result.to_dict(orient="records"),
            "expected_query_result": expected_query_result,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_count_of_entities_to_intersect_given_geometry_to_be_as_predicted(
    query_runner: QueryRunner,
    geometry: str,
    expected_count: list,
    entity_geometry_field: str = "geometry",
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    custom_query = f"SELECT COUNT(*) as count FROM entity WHERE ST_Intersects(GeomFromText({entity_geometry_field}),GeomFromText('{geometry}'));"

    query_result = query_runner.run_query(custom_query)

    result_count = query_result.to_dict(orient="records")[0]["count"]
    result = result_count == expected_count

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result count is not correct, see details"
        details = {
            "result_count": result_count,
            "expected_count": expected_count,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response


def expect_total_count_of_entities_in_dataset_to_be_as_predicted(
    query_runner: QueryRunner,
    expected_count: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    custom_query = "SELECT COUNT(*) as count FROM entity;"

    query_result = query_runner.run_query(custom_query)
    result_count = query_result.to_dict(orient="records")[0]["count"]
    result = result_count == expected_count

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result count is not correct, see details"
        details = {
            "result_count": result_count,
            "expected_count": expected_count,
        }

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )

    return expectation_response

def expect_count_of_entities_in_given_organisations_to_be_as_predicted(
    query_runner: QueryRunner,
    expected_count: int,
    organisation_entities: list,
    expectation_severity: str = "RaiseError",
    **kwargs,
):
    expectation_name = inspect.currentframe().f_code.co_name
    expectation_input = locals()

    custom_query = f"""
        SELECT COUNT(*) as count 
        FROM entity 
        WHERE organisation_entity in ({','.join(organisation_entities)});
    """

    query_result = query_runner.run_query(custom_query)
    result_count = query_result.to_dict(orient="records")[0]["count"]
    result = result_count == expected_count

    details = {
            "result_count": result_count,
            "expected_count": expected_count,
        }
    if result:
        msg = "Success: data quality as expected"
    else:
        msg = "Fail: result count is not correct, see details"

    expectation_response = ExpectationResponse(
        expectation_input=expectation_input,
        result=result,
        msg=msg,
        details=details,
        sqlite_dataset=query_runner.inform_dataset_path(),
    )
