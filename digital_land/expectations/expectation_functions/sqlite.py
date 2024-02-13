import pandas as pd
from ..core import QueryRunner
from math import inf
import logging


def expect_database_to_have_set_of_tables(
    query_runner: QueryRunner,
    expected_tables_set: set,
    fail_if_found_more_than_expected: bool = False,
    **kwargs,
):
    """Receives a set with table names and checks if all of the tables
    can be found in the database. It returns True if all found and False
    if at least one is not found. By default it is one-sided, if you with
    to fail in cases where found more tables than expected change optional
    argument fail_if_found_more_than_expected to True
    """
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

    return result, msg, details


def expect_table_to_have_set_of_columns(
    query_runner: QueryRunner,
    table_name: str,
    expected_columns_set: set,
    fail_if_found_more_than_expected: bool = False,
    **kwargs,
):
    """Receives a table name and a set with column names and checks if all of
    columns can be found in the table. It returns True if all found and False
    if at least one is not found. It doesn't verify if additional columns are
    present in the table.
    """
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

    return result, msg, details


def expect_table_row_count_to_be_in_range(
    query_runner: QueryRunner,
    table_name: str,
    min_expected_row_count: int = 0,
    max_expected_row_count: int = inf,
    **kwargs,
):
    """Receives a table name and a min and max for row count. It returns True
    if the row count is within the range and False otherwise, inclusive of min
    and max.
    """
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

    return result, msg, details


def expect_row_count_for_lookup_value_to_be_in_range(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    count_ranges_per_value: dict,
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

    return result, msg, details


def expect_field_values_to_be_within_set(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    expected_values_set: set,
    fail_if_not_found_entire_expected_set: bool = False,
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

    return result, msg, details


def expect_values_for_field_to_be_unique(
    query_runner: QueryRunner,
    table_name: str,
    fields: list,
    **kwargs,
):
    """Receives a table name, a field (or a set of fields) and checks
    the table doesn't have duplicity for that field (or set of fields).
    Returns True if in the table there are not 2 rows with identical values
    for the field (or set of fields) and False otherwise.
    """
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

    return result, msg, details


def expect_geoshapes_to_be_valid(
    query_runner: QueryRunner,
    table_name: str,
    shape_field: str,
    ref_fields: list,
    **kwargs,
):
    """Receives a table name, a shape field and an shape ref field (or set of)
    checks that the shapes are valid. Returns True if all are valid. Returns
    False if shapes are invalid and in the details returns ref for the shapes
    that were invalid (is_valid=0) or if shape parsing failed unknown(is_valid=-1).
    """

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

    return result, msg, details


def expect_values_for_a_key_stored_in_json_are_within_a_set(
    query_runner: QueryRunner,
    table_name: str,
    field: str,
    json_key: str,
    expected_values_set: set,
    ref_fields: list,
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

    return result, msg, details


def expect_keys_in_json_field_to_be_in_set_of_options(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    expected_keys_set: set,
    ref_fields: list,
    **kwargs,
):
    """Receives a table name, a field name (of a field that has a JSON text
    stored in it) and a set of keys. It checks if the keys found in the JSON
    are within the expected set of expected keys.
    One sided: will not check if all expected keys are found, only if all
    found are within the expected
    """
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

    return result, msg, details


def expect_values_in_field_to_be_within_range(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    min_expected_value: int,
    max_expected_value: int,
    ref_fields: list,
    **kwargs,
):
    """Receives a table name, a field name checks the values found in the field
    are within the expected range
    """
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

    return result, msg, details


def expect_custom_query_result_to_be_as_predicted(
    query_runner: QueryRunner,
    custom_query: str,
    expected_query_result: list,
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

    return result, msg, details


def expect_urls_stored_for_a_key_in_json_to_end_in_expected_domain_endings(
    query_runner: QueryRunner,
    table_name: str,
    field_name: str,
    json_key: str,
    list_of_domain_endings: list,
    ref_fields: list,
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

    return result, msg, details


def expect_entities_to_intersect_given_geometry_to_be_as_predicted(
    query_runner: QueryRunner,
    geometry: str,
    expected_result: list,
    entity_geometry_field: str = "geometry",
    columns: list = ["name"],
    **kwargs,
):
    custom_query = f"""
        {build_entity_select_statement(columns)}
        FROM entity
        WHERE ST_Intersects(GeomFromText({entity_geometry_field}),GeomFromText('{geometry}'))
        ;"""

    query_result = query_runner.run_query(custom_query)

    result = query_result.to_dict(orient="records") == expected_result

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result for lasso query was not as expected, see details"
        details = {
            "query_result": query_result.to_dict(orient="records"),
            "expected_query_result": expected_result,
        }

    return result, msg, details


def expect_count_of_entities_to_intersect_given_geometry_to_be_as_predicted(
    query_runner: QueryRunner,
    geometry: str,
    expected_result: list,
    entity_geometry_field: str = "geometry",
    **kwargs,
):
    custom_query = f"SELECT COUNT(*) as count FROM entity WHERE ST_Intersects(GeomFromText({entity_geometry_field}),GeomFromText('{geometry}'));"

    query_result = query_runner.run_query(custom_query)

    actual_result = query_result.to_dict(orient="records")[0]["count"]
    result = actual_result == expected_result

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result count is not correct, see details"
        details = {
            "result_result": actual_result,
            "expected_result": expected_result,
        }

    return result, msg, details


def expect_total_count_of_entities_in_dataset_to_be_as_predicted(
    query_runner: QueryRunner,
    expected_result: list,
    **kwargs,
):
    custom_query = "SELECT COUNT(*) as count FROM entity;"

    query_result = query_runner.run_query(custom_query)
    actual_result = query_result.to_dict(orient="records")[0]["count"]
    result = actual_result == expected_result

    if result:
        msg = "Success: data quality as expected"
        details = None
    else:
        msg = "Fail: result count is not correct, see details"
        details = {
            "actual_result": actual_result,
            "expected_result": expected_result,
        }

    return result, msg, details


def expect_count_of_entities_in_given_organisations_to_be_as_predicted(
    query_runner: QueryRunner,
    expected_result: int,
    organisation_entities: list,
    **kwargs,
):
    custom_query = f"""
        SELECT COUNT(*) as count
        FROM entity
        WHERE organisation_entity in ({','.join(map(str,organisation_entities))});
    """

    query_result = query_runner.run_query(custom_query)
    actual_result = query_result.to_dict(orient="records")[0]["count"]
    result = actual_result == expected_result

    details = {
        "actual_result": actual_result,
        "expected_result": expected_result,
    }
    if result:
        msg = "Success: data quality as expected"
    else:
        msg = "Fail: result count is not correct, see details"

    return result, msg, details


def expect_filtered_entities_to_be_as_predicted(
    query_runner: QueryRunner,
    expected_result: list,
    columns=None,
    filters: dict = None,
    **kwargs,
):
    possible_filters = ["organisation_entity", "reference"]
    if any(filter not in possible_filters for filter in filters.keys()):
        for filter in filters.keys():
            logging.warning(filter not in possible_filters)
        raise ValueError("unsupported filters being used")

    custom_query = f"""
        {build_entity_select_statement(columns)}
        FROM entity
        {build_entity_where_clause(filters)}
        ;
    """

    actual_result = query_runner.run_query(custom_query).to_dict(orient="records")
    result = actual_result == expected_result

    details = {
        "actual_result": actual_result,
        "expected_result": expected_result,
    }
    if result:
        msg = "Success: data quality as expected"
    else:
        msg = "Fail: result count is not correct, see details"

    return result, msg, details


def build_entity_select_statement(columns):
    """
    function to be used to build a select statement for the entity table
    in the dataset created via the dataset package. Specifically to help
    pull out information stored in the json fields
    """
    possible_entity_columns = [
        "dataset",
        "end_date",
        "entity",
        "entry_date",
        "geojson",
        "geometry",
        "json",
        "name",
        "organisation_entity",
        "point",
        "prefix",
        "reference",
        "start_date",
        "typology",
    ]

    entity_columns = [col for col in columns if col in possible_entity_columns]
    json_columns = [
        f"json_extract(json,'$.{col}') as '{col}'"
        for col in columns
        if col not in possible_entity_columns
    ]
    final_columns = [*entity_columns, *json_columns]
    select_statement = f"SELECT {','.join(final_columns)}"
    return select_statement


def build_entity_where_clause(filters):
    """
    Function to produce sql WHERE clause for the entity table given a dictionary of filters
    """
    possible_filters = [
        "dataset",
        "entity",
        "geometry",
        "json",
        "name",
        "organisation_entity",
        "point",
        "prefix",
        "reference",
        "typology",
    ]

    if any(filter not in possible_filters for filter in filters.keys()):
        unsupported_filters = [
            filter for filter in filters.keys() if filter not in possible_filters
        ]
        raise ValueError(
            f'unsupported filters {",".join(unsupported_filters)} being used'
        )

    for filter in filters.keys():
        filter_values = filters[filter]
        filter_conditions = []
        if filter in ["geometry", "point"]:
            if isinstance(filter_values, str):
                filter_conditions.append(
                    f"ST_Intersects(GeomFromText({filter}),GeomFromText('{filter_values}'))"
                )
            else:
                raise TypeError(f"{filter} value must be a single wkt as a string")
        elif isinstance(filter_values, str) or isinstance(filter_values, int):
            filter_values = f"{filter_values}".replace("'", "''")
            filter_conditions.append(f"{filter} = '{filter_values}'")
        elif isinstance(filter_values, list):
            filter_values = [
                filter_value.replace("'", "''") for filter_value in filter_values
            ]
            filter_values = [f"'{filter_value}'" for filter_value in filter_values]
            filter_conditions.append(f"{filter} in ({','.join(filter_values)})")
        else:
            raise TypeError(f"{filter} must be either an int, str or a list")

    where_clause_sql = f"WHERE {' AND '.join(filter_conditions)}"

    return where_clause_sql


def count_entities(
    query_runner: QueryRunner,
    expected_result: list,
    filters: dict,
    assertion_rule: str = "equals_to",
    **kwargs,
):
    """
    Function which counts entities from the entity table in a sqlite file and compares against
    the expected result

    filters: provide a dictionary contain any filters to be applied to the dataset, geometry and point are converted
    assertion_rule: a string describing how to compare the actual result to the expected result. Currently accepts values equals_to and greater_than
    """
    assertion_rule_set = ["equals_to", "greater_than"]
    if assertion_rule not in assertion_rule_set:
        raise ValueError(f"assetion rule must be in {','.join(assertion_rule_set)}")

    custom_query = f"""
        SELECT COUNT(*) as count
        FROM entity
        {build_entity_where_clause(filters)}
        ;
    """

    query_result = query_runner.run_query(custom_query)
    actual_result = query_result.to_dict(orient="records")[0]["count"]

    if assertion_rule == "equals_to":
        result = actual_result == expected_result
    elif assertion_rule == "greater_than":
        result = actual_result > expected_result

    details = {
        "actual_result": actual_result,
        "expected_result": expected_result,
    }
    if result:
        msg = "Success: data quality as expected"
    else:
        msg = "Fail: result count is not correct, see details"

    return result, msg, details


def compare_entities(
    query_runner: QueryRunner,
    expected_result: list,
    filters: dict,
    columns=None,
    **kwargs,
):
    """
    Function to return a list of of entities which match the provided query

    columns: a list of the columns which need to be returned
    filters:
    name: a human readable shorthand name for the expectation
    description: a human readable description field for the expectation
    """

    custom_query = f"""
        {build_entity_select_statement(columns)}
        FROM entity
        {build_entity_where_clause(filters)}
        ;
    """

    actual_result = query_runner.run_query(custom_query).to_dict(orient="records")
    result = actual_result == expected_result

    details = {
        "actual_result": actual_result,
        "expected_result": expected_result,
    }
    if result:
        msg = "Success: data quality as expected"
    else:
        msg = "Fail: result is not correct, see details"

    return result, msg, details


def compare_column_values(
    query_runner: QueryRunner, expected_result: list, col_1: dict, col_2: dict, **kwargs
):
    """
    Function to compare the values of two columns, returns the rows from table
    1 which intersect the given table 2 column
    """

    custom_query = f"""
    SELECT t1.old_entity, t1.entity, t1.status
    FROM {col_1['table']} AS t1
    INNER JOIN {col_2['table']} AS t2
    ON t1.{col_1['col']} = t2.{col_2['col']}
    ;
    """
    actual_result = query_runner.run_query(custom_query).to_dict(orient="records")
    result = actual_result == expected_result
    details = {"actual_result": actual_result, "expected_result": expected_result}

    if result:
        msg = "Sucess"
    else:
        msg = "Fails"

    return result, msg, details


def validate_wkt_values(
    query_runner: QueryRunner, col: str, table: str, include_cols: list = None, **kwargs
):
    """
    A function that checks a given column within a table contains valid WKT values by converting to to geometry
    and checking geometry is valid

    Parameters:
        query_runner (QueryRunner): an object that is used to send queries to the sqlite db
        column (str): the name of the column containing the wkt to validate
        table (str): the table that the column is contained in
        include_col (list): a list of additional columns to include in output
    """
    if include_cols:
        column_string = ",".join(include_cols)
        column_string = (
            f"{column_string}, ST_IsValidReason(GeomFromText({col})) as reason, {col}"
        )
    else:
        column_string = f"ST_IsValidReason(GeomFromText({col})) as reason, {col}"

    custom_query = f"""
    SELECT {column_string}
    FROM {table}
    WHERE ST_IsValid(GeomFromText({col})) <> 1
    ;
    """

    invalid_wkts = query_runner.run_query(custom_query).to_dict(orient="records")
    result = not len(invalid_wkts) > 0
    if result:
        msg = "Sucess"
    else:
        msg = "Fails"

    details = {"invalid_wkts": invalid_wkts}

    return result, msg, details


def check_old_entities(query_runner: QueryRunner, **kwargs):
    entities_in_old = query_runner.run_query(
        "SELECT entity FROM entity WHERE entity IN (SELECT entity FROM old_entity)"
    )

    result = not len(entities_in_old) > 0

    if result:
        msg = "No enities found in old-entities"
    else:
        msg = "Enitities found in old-entities"

    details = [
        {"entity": entity, "message": "Entity is in old-entities"}
        for entity in entities_in_old.values.flatten()
    ]

    return result, msg, details
