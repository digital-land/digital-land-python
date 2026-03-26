from pathlib import Path


def _read_csv(file_path: Path) -> str:
    return f"read_csv_auto('{str(file_path)}',all_varchar=true,delim=',',quote='\"',escape='\"')"


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


def check_lookup_entities_are_within_organisation_ranges(
    conn, file_path: Path, organisation_file: Path, ignored_organisations: list = None
):
    """
    Checks that lookup entities are within any valid range from an organisation file.

    Args:
        conn: duckdb connection
        file_path: path to the lookup CSV file
        organisation_file: path to the entity-organisation CSV file
        ignored_organisations: list of organisations to ignore (i.e. not check that their entities are within a valid range)
    """
    ignored_values = [
        org.replace("'", "''")
        for org in (ignored_organisations or [])
        if isinstance(org, str) and org.strip()
    ]
    ignored_clause = ""
    if ignored_values:
        ignored_values_sql = ",".join("'" + org + "'" for org in ignored_values)
        ignored_clause = (
            " AND TRIM(COALESCE(\"organisation\", '')) NOT IN "
            + f"({ignored_values_sql})"
        )

    result = conn.execute(
        f"""
        WITH ranges AS (
            SELECT
                TRY_CAST("entity-minimum" AS BIGINT) AS min_entity,
                TRY_CAST("entity-maximum" AS BIGINT) AS max_entity
            FROM {_read_csv(organisation_file)}
            WHERE TRY_CAST("entity-minimum" AS BIGINT) IS NOT NULL
              AND TRY_CAST("entity-maximum" AS BIGINT) IS NOT NULL
        ),
        lookup_rows AS (
            SELECT
                TRY_CAST("entity" AS BIGINT) AS entity,
                TRIM(COALESCE("organisation", '')) AS organisation,
                COALESCE("reference", '') AS reference
            FROM {_read_csv(file_path)}
            WHERE TRIM(COALESCE("organisation", '')) != ''
            {ignored_clause}
        )
        SELECT entity, organisation, reference
        FROM lookup_rows l
        WHERE organisation != ''
          AND entity IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM ranges r
              WHERE l.entity BETWEEN r.min_entity AND r.max_entity
          )
        """
    ).fetchall()

    out_of_range_rows = [
        {"entity": row[0], "organisation": row[1], "reference": row[2]}
        for row in result
    ]

    if len(out_of_range_rows) == 0:
        passed = True
        message = "all lookup entities are within allowed ranges"
    else:
        passed = False
        message = f"there were {len(out_of_range_rows)} out-of-range rows found"

    details = {
        "invalid_rows": out_of_range_rows,
    }

    return passed, message, details

