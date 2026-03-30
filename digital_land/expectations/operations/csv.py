from pathlib import Path
import pandas as pd

from digital_land.expectations.operations.datatype_validators import (
    _is_valid_address_value,
    _is_valid_curie_list_value,
    _is_valid_curie_value,
    _is_valid_datetime_value,
    _is_valid_decimal_value,
    _is_valid_flag_value,
    _is_valid_hash_value,
    _is_valid_integer_value,
    _is_valid_json_value,
    _is_valid_latitude_value,
    _is_valid_longitude_value,
    _is_valid_multipolygon_value,
    _is_valid_pattern_value,
    _is_valid_point_value,
    _is_valid_reference_value,
    _is_valid_url_value,
)


def _read_csv(file_path: Path) -> str:
    return f"read_csv_auto('{str(file_path)}',all_varchar=true,delim=',',quote='\"',escape='\"')"


def _get_csv_columns(conn, file_path: Path) -> list:
    """Get column names from CSV file."""
    return [
        col[0]
        for col in conn.execute(
            f"SELECT * FROM {_read_csv(file_path)} LIMIT 0"
        ).description
    ]


def _sql_string(value) -> str:
    cleaned = str(value).strip().replace("'", "''")
    return f"'{cleaned}'"


def _normalize_condition_groups(conditions, name: str) -> list:
    if conditions is None:
        return []
    if isinstance(conditions, dict):
        return [conditions]
    if isinstance(conditions, list):
        return conditions
    raise ValueError(f"{name} must be a dict, list of dicts, or None")


def _build_field_condition(field_name: str, spec) -> str:
    if isinstance(spec, dict):
        op = str(spec.get("op", spec.get("operation", ""))).strip().lower()
        value = spec.get("value")
        if not op:
            raise ValueError(
                f"Condition for '{field_name}' must include 'op' when using dict format"
            )
    else:
        op = "="
        value = spec

    if op in ("=", "=="):
        return f'"{field_name}" = {_sql_string(value)}'
    if op in ("!=", "<>"):
        return f'"{field_name}" != {_sql_string(value)}'
    if op in ("in", "not in"):
        if not isinstance(value, (list, tuple, set)) or len(value) == 0:
            raise ValueError(
                f"Condition for '{field_name}' with op '{op}' must use a non-empty list"
            )
        values_sql = ", ".join(_sql_string(item) for item in value)
        return f'"{field_name}" {op.upper()} ({values_sql})'

    raise ValueError(
        f"Unsupported operator '{op}' for field '{field_name}'. Supported: =, !=, in, not in"
    )


def _build_condition_group(group: dict, file_columns: list) -> str:
    if not isinstance(group, dict) or not group:
        raise ValueError("Each condition group must be a non-empty dict")

    parts = []
    for field_name, spec in group.items():
        if field_name not in file_columns:
            raise ValueError(
                f"Column '{field_name}' not found in file. Available columns: {file_columns}"
            )
        parts.append(_build_field_condition(field_name, spec))

    return f"({' AND '.join(parts)})"


def _build_filter_clause(filter_spec, file_columns: list, name: str) -> str:
    """Build SQL clause that keeps rows matching structured conditions."""
    groups = _normalize_condition_groups(filter_spec, name)
    if not groups:
        return ""
    clauses = [_build_condition_group(group, file_columns) for group in groups]
    return f" AND ({' OR '.join(clauses)})"


def _normalize_fields_for_validation(field_spec, file_columns: list) -> list:
    """Normalize a field spec into a list of column names to validate."""
    if isinstance(field_spec, str):
        fields = [item.strip() for item in field_spec.split(",") if item.strip()]
    elif isinstance(field_spec, (list, tuple, set)):
        fields = [str(item).strip() for item in field_spec if str(item).strip()]
    else:
        raise ValueError(
            "field must be a string, comma-separated string, or list of strings"
        )

    if not fields:
        raise ValueError("field must include at least one column name")

    seen = set()
    normalized_fields = []
    for field_name in fields:
        if field_name not in seen:
            seen.add(field_name)
            normalized_fields.append(field_name)

    missing_fields = [
        field_name for field_name in normalized_fields if field_name not in file_columns
    ]
    if missing_fields:
        raise ValueError(
            f"Column(s) {missing_fields} not found in file. Available columns: {file_columns}"
        )

    return normalized_fields


def _build_range_invalid_rows(
    result: list,
    validating_multiple_fields: bool,
) -> list:
    """Format query rows into expectation invalid_rows shape."""
    out_of_range_rows = []

    for row in result:
        field_name = row[1]

        invalid_row = {"line_number": row[0], "value": row[2]}
        if validating_multiple_fields:
            invalid_row["field"] = field_name

        out_of_range_rows.append(invalid_row)

    return out_of_range_rows


def count_rows(
    conn, file_path: Path, expected: int, comparison_rule: str = "greater_than"
):
    """
    Counts the number of rows in the CSV and compares against an expected value.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
        expected: the expected row count
        comparison_rule: how to compare actual vs expected
    """
    result = conn.execute(f"SELECT COUNT(*) FROM {_read_csv(file_path)}").fetchone()
    actual = result[0]

    comparison_rules = {
        "equals_to": actual == expected,
        "not_equal_to": actual != expected,
        "greater_than": actual > expected,
        "greater_than_or_equal_to": actual >= expected,
        "less_than": actual < expected,
        "less_than_or_equal_to": actual <= expected,
    }

    if comparison_rule not in comparison_rules:
        raise ValueError(
            f"Invalid comparison_rule: '{comparison_rule}'. Must be one of {list(comparison_rules.keys())}."
        )

    passed = comparison_rules[comparison_rule]
    message = f"there were {actual} rows found"
    details = {
        "actual": actual,
        "expected": expected,
    }

    return passed, message, details


def check_unique(conn, file_path: Path, field: str):
    """
    Checks that all values in a given field are unique.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
        field: the column name to check for uniqueness
    """
    result = conn.execute(
        f'SELECT "{field}", COUNT(*) as cnt FROM {_read_csv(file_path)} GROUP BY "{field}" HAVING cnt > 1'
    ).fetchall()

    duplicates = [{"value": row[0], "count": row[1]} for row in result]

    if len(duplicates) == 0:
        passed = True
        message = f"all values in '{field}' are unique"
    else:
        passed = False
        message = f"there were {len(duplicates)} duplicate values in '{field}'"

    details = {
        "field": field,
        "duplicates": duplicates,
    }

    return passed, message, details


def check_no_shared_values(conn, file_path: Path, field_1: str, field_2: str):
    """
    Checks that no value appears in both field_1 and field_2.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
        field_1: the first column name
        field_2: the second column name
    """
    result = conn.execute(
        f"""
        SELECT DISTINCT a."{field_1}" as value
        FROM {_read_csv(file_path)} a
        WHERE a."{field_1}" IN (SELECT "{field_2}" FROM {_read_csv(file_path)})
        AND a."{field_1}" IS NOT NULL AND a."{field_1}" != ''
        """
    ).fetchall()

    shared_values = [row[0] for row in result]

    if len(shared_values) == 0:
        passed = True
        message = f"no shared values between '{field_1}' and '{field_2}'"
    else:
        passed = False
        message = f"there were {len(shared_values)} shared values between '{field_1}' and '{field_2}'"

    details = {
        "field_1": field_1,
        "field_2": field_2,
        "shared_values": shared_values,
    }

    return passed, message, details


def check_no_overlapping_ranges(conn, file_path: Path, min_field: str, max_field: str):
    """
    Checks that no ranges overlap between rows.

    Two ranges [a_min, a_max] and [b_min, b_max] overlap if:
    a_min <= b_max AND a_max >= b_min

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
        min_field: the column name for the range minimum
        max_field: the column name for the range maximum
    """
    result = conn.execute(
        f"""
        SELECT
            a."{min_field}" as a_min,
            a."{max_field}" as a_max,
            b."{min_field}" as b_min,
            b."{max_field}" as b_max
        FROM {_read_csv(file_path)} a
        JOIN {_read_csv(file_path)} b
        ON CAST(a."{min_field}" AS BIGINT) < CAST(b."{min_field}" AS BIGINT)
        WHERE CAST(a."{min_field}" AS BIGINT) <= CAST(b."{max_field}" AS BIGINT)
        AND CAST(a."{max_field}" AS BIGINT) >= CAST(b."{min_field}" AS BIGINT)
        """
    ).fetchall()

    overlaps = [
        {"range_1": [row[0], row[1]], "range_2": [row[2], row[3]]} for row in result
    ]

    if len(overlaps) == 0:
        passed = True
        message = f"no overlapping ranges found between '{min_field}' and '{max_field}'"
    else:
        passed = False
        message = f"there were {len(overlaps)} overlapping ranges found"

    details = {
        "min_field": min_field,
        "max_field": max_field,
        "overlaps": overlaps,
    }

    return passed, message, details


def check_allowed_values(conn, file_path: Path, field: str, allowed_values: list):
    """
    Checks that a field contains only values from an allowed set.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
        field: the column name to validate
        allowed_values: allowed values for the field
    """
    cleaned_allowed_values = [
        str(value).strip().replace("'", "''")
        for value in (allowed_values or [])
        if str(value).strip() != ""
    ]

    if not cleaned_allowed_values:
        raise ValueError("allowed_values must contain at least one non-empty value")

    allowed_values_sql = ",".join("'" + value + "'" for value in cleaned_allowed_values)

    result = conn.execute(
        f"""
        SELECT
            ROW_NUMBER() OVER () + 1 AS line_number,
            TRIM(COALESCE("{field}", '')) AS value
        FROM {_read_csv(file_path)}
        WHERE TRIM(COALESCE("{field}", '')) NOT IN ({allowed_values_sql})
        """
    ).fetchall()

    invalid_rows = [{"line_number": row[0], "value": row[1]} for row in result]
    invalid_values = sorted({row["value"] for row in invalid_rows})

    if len(invalid_rows) == 0:
        passed = True
        message = f"all values in '{field}' are allowed"
    else:
        passed = False
        message = f"there were {len(invalid_rows)} invalid values in '{field}'"

    details = {
        "field": field,
        "allowed_values": sorted({value for value in cleaned_allowed_values}),
        "invalid_values": invalid_values,
        "invalid_rows": invalid_rows,
    }

    return passed, message, details


def check_no_blank_rows(conn, file_path: Path):
    """
    Checks that the CSV does not contain fully blank rows.

    A row is considered blank when every column is empty after trimming whitespace.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file
    """
    file_columns = _get_csv_columns(conn, file_path)
    if not file_columns:
        return True, "no blank rows found", {"invalid_rows": []}

    blank_conditions = " AND ".join(
        f"TRIM(COALESCE(\"{column_name}\", '')) = ''" for column_name in file_columns
    )

    result = conn.execute(
        f"""
        WITH source_rows AS (
            SELECT
                ROW_NUMBER() OVER () + 1 AS line_number,
                *
            FROM {_read_csv(file_path)}
        )
        SELECT
            line_number
        FROM source_rows
        WHERE {blank_conditions}
        ORDER BY line_number
        """
    ).fetchall()

    invalid_rows = [{"line_number": row[0]} for row in result]

    if len(invalid_rows) == 0:
        passed = True
        message = "no blank rows found"
    else:
        passed = False
        message = f"there were {len(invalid_rows)} blank rows found"

    details = {
        "invalid_rows": invalid_rows,
    }
    return passed, message, details


def check_fields_are_within_range(
    conn,
    file_path: Path,
    field: str,
    external_file: Path,
    min_field: str,
    max_field: str,
    rules: dict = None,
):
    """
    Check that one or more lookup fields are within ranges from an external file.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file containing fields to validate
        field: column name(s) to validate.
               You can pass a single name ("entity") or a comma-separated list
               ("entity, end-entity"). All specified fields must be within range.
        external_file: path to the CSV file containing valid ranges
        min_field: the column name for the range minimum
        max_field: the column name for the range maximum
        rules: optional dict controlling subset selection on lookup rows.
               Supported keys:
               - lookup_rules: dict or list[dict] of structured conditions.
                 Fields in one dict are AND'ed; multiple dicts are OR'ed.
               Examples:
               {"lookup_rules": {"prefix": "conservationarea"}}
               {"lookup_rules": {"organisation": {"op": "in", "value": ["orgA", "orgB"]}}}
               Use operators like != and not in when you want to exclude rows.
    """
    file_columns = _get_csv_columns(conn, file_path)
    rules = rules or {}
    if not isinstance(rules, dict):
        raise ValueError("rules must be a dictionary or None")

    lookup_clause = _build_filter_clause(
        rules.get("lookup_rules"),
        file_columns,
        "rules.lookup_rules",
    )

    fields_to_validate = _normalize_fields_for_validation(field, file_columns)
    validating_multiple_fields = len(fields_to_validate) > 1
    lookup_values_sql = ",\n                    ".join(
        f'({i}, {_sql_string(field_name)}, TRY_CAST(src."{field_name}" AS BIGINT))'
        for i, field_name in enumerate(fields_to_validate)
    )

    result = conn.execute(
        f"""
        WITH ranges AS (
            SELECT
                TRY_CAST("{min_field}" AS BIGINT) AS min_value,
                TRY_CAST("{max_field}" AS BIGINT) AS max_value
            FROM {_read_csv(external_file)}
            WHERE TRY_CAST("{min_field}" AS BIGINT) IS NOT NULL
              AND TRY_CAST("{max_field}" AS BIGINT) IS NOT NULL
        ),
        source_rows AS (
            SELECT
                ROW_NUMBER() OVER () + 1 AS line_number,
                *
            FROM {_read_csv(file_path)}
        ),
        lookup_rows AS (
            SELECT
                src.line_number,
                fields.field_order,
                fields.field_name,
                fields.value
            FROM source_rows src
            CROSS JOIN LATERAL (
                VALUES
                    {lookup_values_sql}
            ) AS fields(field_order, field_name, value)
            WHERE fields.value IS NOT NULL{lookup_clause}
        )
        SELECT
            line_number,
            field_name,
            value
        FROM lookup_rows l
        WHERE NOT EXISTS (
            SELECT 1
            FROM ranges r
            WHERE l.value BETWEEN r.min_value AND r.max_value
        )
        ORDER BY field_order, line_number
        """
    ).fetchall()

    out_of_range_rows = _build_range_invalid_rows(
        result=result,
        validating_multiple_fields=validating_multiple_fields,
    )

    if len(out_of_range_rows) == 0:
        passed = True
        message = f"all values in '{field}' are within allowed ranges"
    else:
        passed = False
        message = f"there were {len(out_of_range_rows)} out-of-range rows found"

    details = {"invalid_rows": out_of_range_rows}
    return passed, message, details


def check_field_is_within_range_by_dataset_org(
    conn,
    file_path: Path,
    field: str,
    external_file: Path,
    min_field: str,
    max_field: str,
    lookup_dataset_field: str,
    range_dataset_field: str,
    rules: dict = None,
):
    """
    Check field values are within ranges matched by dataset field and organisation.

    Matching is fixed to two keys:
    1. lookup_dataset_field -> range_dataset_field
    2. organisation -> organisation

    Args:
        conn: duckdb connection
        file_path: path to the CSV file containing fields to validate
        field: single column name to validate (for example: "entity").
        external_file: path to the CSV file containing valid ranges
        min_field: the column name for the range minimum
        max_field: the column name for the range maximum
        lookup_dataset_field: dataset column name in file_path
        range_dataset_field: dataset column name in external_file
        rules: optional dict controlling subset selection on lookup rows.
               Supported keys:
               - lookup_rules: dict or list[dict] of structured conditions.
                 Fields in one dict are AND'ed; multiple dicts are OR'ed.
               Examples:
               {"lookup_rules": {"prefix": "conservationarea"}}
               {"lookup_rules": {"organisation": {"op": "in", "value": ["orgA", "orgB"]}}}
               Use operators like != and not in when you want to exclude rows.
    """
    file_columns = _get_csv_columns(conn, file_path)
    rules = rules or {}
    if not isinstance(rules, dict):
        raise ValueError("rules must be a dictionary or None")

    lookup_clause = _build_filter_clause(
        rules.get("lookup_rules"),
        file_columns,
        "rules.lookup_rules",
    )

    fields_to_validate = _normalize_fields_for_validation(field, file_columns)
    if len(fields_to_validate) != 1:
        raise ValueError("field must be a single column name")
    field_name = fields_to_validate[0]

    lookup_dataset_name = str(lookup_dataset_field).strip()
    range_dataset_name = str(range_dataset_field).strip()
    lookup_match_columns = [lookup_dataset_name, "organisation"]

    lookup_dataset_col = f'"{lookup_dataset_name}"'
    range_dataset_col = f'"{range_dataset_name}"'
    min_col = f'"{min_field}"'
    max_col = f'"{max_field}"'
    value_col = f'"{field_name}"'

    result = conn.execute(
        f"""
        WITH ranges AS (
            SELECT
                TRY_CAST({min_col} AS BIGINT) AS min_value,
                TRY_CAST({max_col} AS BIGINT) AS max_value,
                TRIM(COALESCE({range_dataset_col}, '')) AS range_key_0,
                                TRIM(COALESCE("organisation", '')) AS range_key_1
            FROM {_read_csv(external_file)}
            WHERE TRY_CAST({min_col} AS BIGINT) IS NOT NULL
              AND TRY_CAST({max_col} AS BIGINT) IS NOT NULL
              AND TRIM(COALESCE({range_dataset_col}, '')) != ''
                            AND TRIM(COALESCE("organisation", '')) != ''
        ),
        source_rows AS (
            SELECT
                ROW_NUMBER() OVER () + 1 AS line_number,
                *
            FROM {_read_csv(file_path)}
        ),
        lookup_rows AS (
            SELECT
                src.line_number,
                TRY_CAST(src.{value_col} AS BIGINT) AS value,
                TRIM(COALESCE(src.{lookup_dataset_col}, '')) AS lookup_key_0,
                TRIM(COALESCE(src."organisation", '')) AS lookup_key_1
            FROM source_rows src
            WHERE TRY_CAST(src.{value_col} AS BIGINT) IS NOT NULL
              AND TRIM(COALESCE(src.{lookup_dataset_col}, '')) != ''
              AND TRIM(COALESCE(src."organisation", '')) != ''{lookup_clause}
        )
        SELECT
            line_number,
            value,
            lookup_key_0,
            lookup_key_1
        FROM lookup_rows l
        WHERE NOT EXISTS (
            SELECT 1
            FROM ranges r
            WHERE l.value BETWEEN r.min_value AND r.max_value
                  AND l.lookup_key_0 = r.range_key_0
                  AND l.lookup_key_1 = r.range_key_1
        )
        ORDER BY line_number
        """
    ).fetchall()

    out_of_range_rows = []
    for row in result:
        invalid_row = {"line_number": row[0], field_name: row[1]}
        for i, col_name in enumerate(lookup_match_columns):
            invalid_row[col_name] = row[i + 2]
        out_of_range_rows.append(invalid_row)

    if len(out_of_range_rows) == 0:
        passed = True
        message = f"all values in '{field}' are within allowed ranges"
    else:
        passed = False
        message = f"there were {len(out_of_range_rows)} out-of-range rows found"

    details = {"invalid_rows": out_of_range_rows}
    return passed, message, details


def check_values_have_the_correct_datatype(file_path, field_datatype, conn=None):
    """
    Validates that CSV column values have correct datatypes.

    This function uses pandas to read and validate the CSV using datatype validators.
    The conn parameter is accepted for consistency with other operations but not used.

    Args:
        file_path: path to the CSV file to validate
        field_datatype: dict mapping column name to datatype string
    """
    validators = {
        "address": _is_valid_address_value,
        "curie-list": _is_valid_curie_list_value,
        "curie": _is_valid_curie_value,
        "date": _is_valid_datetime_value,
        "datetime": _is_valid_datetime_value,
        "decimal": _is_valid_decimal_value,
        "flag": _is_valid_flag_value,
        "hash": _is_valid_hash_value,
        "integer": _is_valid_integer_value,
        "json": _is_valid_json_value,
        "latitude": _is_valid_latitude_value,
        "longitude": _is_valid_longitude_value,
        "multipolygon": _is_valid_multipolygon_value,
        "pattern": _is_valid_pattern_value,
        "point": _is_valid_point_value,
        "reference": _is_valid_reference_value,
        "url": _is_valid_url_value,
    }

    # Read CSV with pandas (keep_default_na=False preserves empty strings)
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

    if df.empty or len(df.columns) == 0:
        return True, "no invalid values found", {"invalid_rows": []}

    # Identify applicable fields for validation
    applicable_fields = [
        (field, field_datatype.get(field), validators[field_datatype.get(field)])
        for field in df.columns
        if field in field_datatype and field_datatype.get(field) in validators
    ]

    if not applicable_fields:
        return True, "no invalid values found", {"invalid_rows": []}

    # Validate values
    invalid_values = []
    for line_number, (_, row) in enumerate(df.iterrows(), start=2):
        for field, datatype, validator in applicable_fields:
            value = str(row.get(field, "")).strip()
            if not value:
                continue

            if not validator(value):
                invalid_values.append(
                    {
                        "line_number": line_number,
                        "field": field,
                        "datatype": datatype,
                        "value": value,
                    }
                )

    if len(invalid_values) == 0:
        passed = True
        message = "all values have valid datatypes"
        details = {"invalid_rows": []}
    else:
        passed = False
        message = f"there were {len(invalid_values)} invalid datatype value(s) found"
        details = {"invalid_rows": invalid_values}

    return passed, message, details
