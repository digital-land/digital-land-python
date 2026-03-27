from pathlib import Path


def _read_csv(file_path: Path) -> str:
    return f"read_csv_auto('{str(file_path)}',all_varchar=true,delim=',',quote='\"',escape='\"')"


def _get_csv_columns(conn, file_path: Path) -> list:
    """Get column names from CSV file."""
    return [col[0] for col in conn.execute(
        f"SELECT * FROM {_read_csv(file_path)} LIMIT 0"
    ).description]


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


def _build_match_column_sql_parts(columns: list, alias_prefix: str) -> tuple:
    """Build SQL fragments for match-key columns.

    Returns:
        tuple[str, str]:
            - SELECT projection fragment with normalized key aliases.
            - WHERE fragment ensuring key columns are non-empty.
    """
    select_fragment = ",\n                ".join(
        f'TRIM(COALESCE("{column}", \'\')) AS {alias_prefix}_key_{i}'
        for i, column in enumerate(columns)
    )
    non_empty_filter_fragment = "\n              AND ".join(
        f'TRIM(COALESCE("{column}", \'\')) != \'\''
        for column in columns
    )
    return select_fragment, non_empty_filter_fragment


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
        message = (
            f"there were {len(invalid_rows)} invalid values in '{field}'"
        )

    details = {
        "field": field,
        "allowed_values": sorted({value for value in cleaned_allowed_values}),
        "invalid_values": invalid_values,
        "invalid_rows": invalid_rows,
    }

    return passed, message, details


def check_field_is_within_range(
    conn,
    file_path: Path,
    field: str,
    external_file: Path,
    min_field: str,
    max_field: str,
    rules: dict = None,
):
    """
    Checks that a field's values are within any valid range from an external file.

    Args:
        conn: duckdb connection
        file_path: path to the CSV file containing the field to validate
        external_file: path to the CSV file containing the ranges
        min_field: the column name for the range minimum
        max_field: the column name for the range maximum
        field: the column name to validate
        rules: optional dict that controls subset selection and key matching.
                 Supported keys:
                 - lookup_rules: dict or list[dict] of structured conditions for file_path rows.
                 - match_columns: dict with keys {"lookup": [...], "range": [...]} specifying columns to match.
                   lookup columns come from file_path (the rows being validated).
                   range columns come from external_file (the rows providing valid ranges).
                 Examples:
                 {"lookup_rules": {"prefix": "conservationarea"}}
                 {"lookup_rules": {"organisation": {"op": "in", "value": ["orgA", "orgB"]}}}
                 {"match_columns": {"lookup": ["prefix", "organisation"], "range": ["dataset", "organisation"]}}
                 Use operators like != and not in when you want to exclude rows.
    """
    file_cols_list = _get_csv_columns(conn, file_path)
    external_cols_list = _get_csv_columns(conn, external_file)
    rules = rules or {}
    if not isinstance(rules, dict):
        raise ValueError("rules must be a dictionary or None")

    lookup_clause = _build_filter_clause(
        rules.get("lookup_rules"),
        file_cols_list,
        "rules.lookup_rules",
    )

    # Validate and extract match_columns
    file_cols = external_cols = None
    match_columns = rules.get("match_columns")
    if match_columns is not None:
        if not isinstance(match_columns, dict):
            raise ValueError("rules.match_columns must be a dictionary")
        file_cols = match_columns.get("lookup")
        external_cols = match_columns.get("range")
        if file_cols is None or external_cols is None:
            raise ValueError(
                'rules.match_columns must have keys "lookup" and "range" with column lists'
            )
        if not file_cols or not external_cols:
            raise ValueError(
                'rules.match_columns "lookup" and "range" lists must be non-empty'
            )
        if len(file_cols) != len(external_cols):
            raise ValueError(
                'rules.match_columns "lookup" and "range" lists must have the same length'
            )
        for col in file_cols:
            if col not in file_cols_list:
                raise ValueError(
                    f"Column '{col}' not found in file. Available columns: {file_cols_list}"
                )
        for col in external_cols:
            if col not in external_cols_list:
                raise ValueError(
                    f"Column '{col}' not found in external file. Available columns: {external_cols_list}"
                )

    # Simple range check without key matching
    if match_columns is None:
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
            lookup_rows AS (
                SELECT
                    ROW_NUMBER() OVER () + 1 AS line_number,
                    TRY_CAST("{field}" AS BIGINT) AS value
                FROM {_read_csv(file_path)}
                WHERE TRY_CAST("{field}" AS BIGINT) IS NOT NULL{lookup_clause}
            )
            SELECT line_number, value
            FROM lookup_rows l
            WHERE value IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM ranges r
                  WHERE l.value BETWEEN r.min_value AND r.max_value
              )
            """
        ).fetchall()
        out_of_range_rows = [{"line_number": row[0], "value": row[1]} for row in result]
    else:
        # Key-matched range check
        range_key_select_sql, range_keys_non_empty_sql = _build_match_column_sql_parts(
            external_cols, "range"
        )
        lookup_key_select_sql, lookup_keys_non_empty_sql = _build_match_column_sql_parts(
            file_cols, "lookup"
        )
        key_match_condition_sql = "\n                AND ".join(
            f"l.lookup_key_{i} = r.range_key_{i}"
            for i in range(len(file_cols))
        )
        key_projection_sql = ", ".join(
            f"lookup_key_{i}" for i in range(len(file_cols))
        )

        result = conn.execute(
            f"""
            WITH ranges AS (
                SELECT
                                        {range_key_select_sql},
                    TRY_CAST("{min_field}" AS BIGINT) AS min_value,
                    TRY_CAST("{max_field}" AS BIGINT) AS max_value
                FROM {_read_csv(external_file)}
                WHERE TRY_CAST("{min_field}" AS BIGINT) IS NOT NULL
                                    AND TRY_CAST("{max_field}" AS BIGINT) IS NOT NULL
                                    AND {range_keys_non_empty_sql}
            ),
            lookup_rows AS (
                SELECT
                    ROW_NUMBER() OVER () + 1 AS line_number,
                    TRY_CAST("{field}" AS BIGINT) AS value,
                                        {lookup_key_select_sql}
                FROM {_read_csv(file_path)}
                                WHERE {lookup_keys_non_empty_sql} AND TRY_CAST("{field}" AS BIGINT) IS NOT NULL{lookup_clause}
            )
                        SELECT line_number, value, {key_projection_sql}
            FROM lookup_rows l
            WHERE value IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM ranges r
                                    WHERE {key_match_condition_sql}
                    AND l.value BETWEEN r.min_value AND r.max_value
              )
            """
        ).fetchall()

        out_of_range_rows = []
        for row in result:
            invalid_row = {"line_number": row[0], field: row[1]}
            for i, col_name in enumerate(file_cols):
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

