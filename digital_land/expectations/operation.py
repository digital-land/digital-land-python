import requests
import pandas as pd
import urllib
import os
import time


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
        message = f"An error occurred when retrieving lpa geometry from platform {err}"
        details = {}
        return passed, message, details

    # now deal with spatial options
    # Determine the spatial condition based on the geometric_relation parameter
    spatial_options = {
        "within": f"""
            CASE
                WHEN geometry != '' THEN ST_WITHIN(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE ST_WITHIN(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """,
        "intersects": f"""
            CASE
                WHEN geometry != '' THEN ST_INTERSECTS(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE ST_INTERSECTS(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """,
        "not_intersects": f"""
            CASE
                WHEN geometry != '' THEN NOT ST_INTERSECTS(ST_GeomFromText(geometry), ST_GeomFromText('{lpa_geometry}'))
                ELSE NOT ST_INTERSECTS(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
            END
        """,
        "centroid_within": f"""
                CASE
                    WHEN point != '' THEN ST_WITHIN(ST_GeomFromText(point), ST_GeomFromText('{lpa_geometry}'))
                    ELSE ST_WITHIN(ST_CENTROID(ST_GeomFromText(geometry)), ST_GeomFromText('{lpa_geometry}'))
                END
            """,
    }

    if geometric_relation not in spatial_options:
        raise ValueError(
            f"Invalid geometric_relation: '{geometric_relation}'. Must be one of {list(spatial_options.keys())}."
        )

    spatial_condition = spatial_options[geometric_relation]

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
        "not_equal_to": actual != expected,
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

    message = f"there were {actual} entities found"

    details = {
        "actual": actual,
        "expected": expected,
        "entities": entities,
    }

    return result, message, details


def count_deleted_entities(
    conn,
    expected: int,
    organisation_entity: int = None,
):
    # get database name to identify dataset
    db_path = conn.execute("PRAGMA database_list").fetchall()[0][2]
    db_name = os.path.splitext(os.path.basename(db_path))[0]

    # get dataset specific active resource list
    params = urllib.parse.urlencode(
        {
            "sql": f"""select * from reporting_historic_endpoints rhe join organisation o on rhe.organisation=o.organisation
                        where pipeline == '{db_name}' and o.entity='{organisation_entity}' and resource_end_date == "" group by endpoint""",
            "_size": "max",
        }
    )
    base_url = f"https://datasette.planning.data.gov.uk/digital-land.csv?{params}"

    # Can have an issue getting data from datasette. If this occurs then wait a minute and retry
    max_retries = 60  # Retry for an hour
    for attempt in range(max_retries):
        try:
            get_resource = pd.read_csv(base_url)
            break
        except urllib.error.HTTPError:
            time.sleep(60)
    else:
        raise Exception("Failed to fetch datasette after multiple attempts")

    resource_list = get_resource["resource"].to_list()

    # use resource list to get current entities
    query = f"""select e.reference,fe.entry_number,f.entity,e.name,e.organisation_entity
                from fact_resource fe join fact f on fe.fact=f.fact join entity e on f.entity=e.entity
                where resource in ({','.join(f"'{x}'" for x in resource_list)})
                group by reference
    """
    rows = conn.execute(query).fetchall()
    get_active_entities = [row[0] for row in rows]

    # get entities from entity table to compare against resource entities
    query = f"""
    select reference from entity where organisation_entity = '{organisation_entity}';
    """
    rows = conn.execute(query).fetchall()
    get_entities = [row[0] for row in rows]

    # identify entities present in the entity table but missing from the resource
    entities = [item for item in get_entities if item not in get_active_entities]
    actual = len(entities)

    result = bool(actual == expected)
    message = f"there were {actual} entities found"
    details = {
        "actual": actual,
        "expected": expected,
        "entities": entities,
    }

    return result, message, details
