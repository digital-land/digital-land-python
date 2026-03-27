from pathlib import Path


def _read_csv(file_path: Path) -> str:
    return f"read_csv_auto('{str(file_path)}',all_varchar=true,delim=',',quote='\"',escape='\"')"


def _get_csv_columns(conn, file_path: Path) -> list:
    """Get column names from CSV file."""
    return [col[0] for col in conn.execute(
        f"SELECT * FROM {_read_csv(file_path)} LIMIT 0"
    ).description]


def _build_exclude_clause(exclude: list) -> str:
    """Build SQL NOT clause from exclude conditions. Each dict is AND group; list is OR between groups."""
    if not exclude:
        return ""
    exclude_conditions = []
    for exclude_dict in exclude:
        and_parts = []
        for k, v in exclude_dict.items():
            cleaned = str(v).strip().replace("'", "''")
            and_parts.append(f'"{k}" = \'{cleaned}\'')
        if and_parts:
            exclude_conditions.append(f"({' AND '.join(and_parts)})")
    return f" AND NOT ({' OR '.join(exclude_conditions)})" if exclude_conditions else ""


def _build_key_sql(cols: list, prefix: str) -> tuple:
    """Build SQL key SELECT and WHERE fragments. Returns (select, where_not_empty)."""
    select = ",\n                ".join(
        f'TRIM(COALESCE("{col}", \'\')) AS {prefix}_key_{i}'
        for i, col in enumerate(cols)
    )
    where = "\n              AND ".join(
        f'TRIM(COALESCE("{col}", \'\')) != \'\''
        for col in cols
    )
    return select, where


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
    join_on: dict = None,
    exclude: list = None,
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
        join_on: optional dict with keys {"file": [...], "external": [...]} specifying columns to match for range validation
        exclude: optional list of dicts specifying row conditions to exclude from validation. Each dict is an AND group; the list is OR between groups.
                 Example: [{"prefix": "conservationarea", "organisation": "orgA"}, {"prefix": "conservationarea", "organisation": "orgB"}]
    """
    file_cols_list = _get_csv_columns(conn, file_path)
    external_cols_list = _get_csv_columns(conn, external_file)
    exclude_clause = _build_exclude_clause(exclude)

    # Validate and extract join_on
    file_cols = external_cols = None
    if join_on is not None:
        if not isinstance(join_on, dict):
            raise ValueError("join_on must be a dictionary")
        file_cols = join_on.get("file")
        external_cols = join_on.get("external")
        if file_cols is None or external_cols is None:
            raise ValueError(
                'join_on must have keys "file" and "external" with column lists'
            )
        if not file_cols or not external_cols:
            raise ValueError(
                'join_on "file" and "external" lists must be non-empty'
            )
        if len(file_cols) != len(external_cols):
            raise ValueError(
                'join_on "file" and "external" lists must have the same length'
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
    if join_on is None:
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
                WHERE TRY_CAST("{field}" AS BIGINT) IS NOT NULL{exclude_clause}
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
        range_keys, range_empty = _build_key_sql(external_cols, "range")
        lookup_keys, lookup_empty = _build_key_sql(file_cols, "lookup")
        key_join = "\n                AND ".join(
            f"l.lookup_key_{i} = r.range_key_{i}"
            for i in range(len(file_cols))
        )
        key_proj = ", ".join(f"lookup_key_{i}" for i in range(len(file_cols)))

        result = conn.execute(
            f"""
            WITH ranges AS (
                SELECT
                    {range_keys},
                    TRY_CAST("{min_field}" AS BIGINT) AS min_value,
                    TRY_CAST("{max_field}" AS BIGINT) AS max_value
                FROM {_read_csv(external_file)}
                WHERE TRY_CAST("{min_field}" AS BIGINT) IS NOT NULL
                  AND TRY_CAST("{max_field}" AS BIGINT) IS NOT NULL
                  AND {range_empty}
            ),
            lookup_rows AS (
                SELECT
                    ROW_NUMBER() OVER () + 1 AS line_number,
                    TRY_CAST("{field}" AS BIGINT) AS value,
                    {lookup_keys}
                FROM {_read_csv(file_path)}
                WHERE {lookup_empty} AND TRY_CAST("{field}" AS BIGINT) IS NOT NULL{exclude_clause}
            )
            SELECT line_number, value, {key_proj}
            FROM lookup_rows l
            WHERE value IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM ranges r
                  WHERE {key_join}
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

