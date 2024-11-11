import requests


# # TODO is there a way to represent this in a generalised count or not
def count_lpa_boundary(
    conn,
    lpa: str,
    expected: int,
    organisation_entity: int = None,
    comparison_rule: str = "equals_to",
    geometric_relation: str = "within",
):
    """
    Specific version of a count which given a local authority
    and a dataset checks for  any entities relating to the lpa boundary.
    relation defaults to within but can be changed.  This should only be used on geographic
    datasets
    args:
        conn: sqlite connection used to connect to the db, wil be created by the checkpoint class
        lpa: The reference to the local planning authority (geography dataset) boundary  to use
        expected: the expected count, must be a non-negative integer
        organisation: optional additional filter to  filter by  organisation_entity as well as boundary
        geometric_relation: how to decide if the data is related to the lpa boundary
    """
    # get lpa boundary
    # get geometric boundary from API
    # TODO should be moved to the sdk/api for accessing the platform
    try:
        base_url = "https://www.planning.data.gov.uk"
        endpoint = f"curie/statistical-geography:{lpa}.json"
        response = requests.get(
            f"{base_url}/{endpoint}",
        )
        response.raise_for_status()
        data = response.json()
        lpa_geometry = data["geometry"]
    except requests.exceptions.RequestException as err:
        passed = False
        message = f"An error occured when retrieving lpa geometry from platform {err}"
        details = {}
        return passed, message, details

    # now deal with spatial options
    # Determine the spatial condition based on the geometric_relation parameter
    if geometric_relation == "within":
        spatial_condition = f"""
            CASE
                WHEN geometry != '' THEN ST_WITHIN(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE ST_WITHIN(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """
    elif geometric_relation == "intersects":
        spatial_condition = f"""
            CASE
                WHEN geometry != '' THEN ST_INTERSECTS(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE ST_INTERSECTS(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """
    elif geometric_relation == "not_intersects":
        spatial_condition = f"""
            CASE
                WHEN geometry != '' THEN NOT ST_INTERSECTS(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE NOT ST_INTERSECTS(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """
    else:
        raise ValueError(
            f"Invalid geometric_relation: '{geometric_relation}'. Must be one of ['within', 'intersects', 'not_intersects']."
        )

    # set up initial query
    query = """
        SELECT entity
        FROM entity
        WHERE (geometry != '' OR point != '')
    """

    if organisation_entity:
        query = query + f"AND organisation_entity = '{organisation_entity}'"

    query = query + f"AND ({spatial_condition});"
    rows = conn.execute(query).fetchall()
    entities = [row[0] for row in rows]
    actual = len(entities)

    # compare expected to actual
    # Define comparison rules
    comparison_rules = {
        "equals_to": actual == expected,
        "greater_than": actual > expected,
        "greater_than_or_equal_to": actual >= expected,
        "less_than": actual < expected,
        "less_than_or_equal_to": actual <= expected,
    }

    # Perform comparison based on the specified operator
    if comparison_rule not in comparison_rules:
        raise ValueError(
            f"Invalid comparison_operator: '{comparison_rule}'. Must be one of {list(comparison_rules.keys())}."
        )

    result = comparison_rules[comparison_rule]

    message = f"there were {actual} enttities found"

    details = {
        "actual": actual,
        "expected": expected,
        "entities": entities,
    }

    return result, message, details
