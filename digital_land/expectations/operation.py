import sqlite3
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
    query = f"""select f.entity
                from fact_resource fe join fact f on fe.fact=f.fact join entity e on f.entity=e.entity
                where resource in ({','.join(f"'{x}'" for x in resource_list)})
                group by reference
    """
    rows = conn.execute(query).fetchall()
    get_active_entities = [row[0] for row in rows]

    # get entities from entity table to compare against resource entities
    query = f"""
    select entity from entity where organisation_entity = '{organisation_entity}';
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


def check_columns(conn, expected: dict):
    # This operation checks that the db connection provided contains the tables with the expected columns provided

    # expected: a dictionary containing table names as keys, with a list of their expected columns as the value

    details = []
    success_count = 0
    failure_count = 0
    for k, v in expected.items():
        table_name = k
        expected_columns = v
        sql = f"""
        PRAGMA table_info({table_name})
        """
        rows = conn.execute(sql).fetchall()
        actual = [row[1] for row in rows]
        success = set(expected_columns).issubset(set(actual))
        missing = list(set(expected_columns) - set(actual))
        details.append(
            {
                "table": table_name,
                "success": success,
                "missing": missing,
                "actual": actual,
                "expected": expected_columns,
            }
        )
        if success:
            success_count += 1
        else:
            failure_count += 1

    result = False if failure_count > 0 else True
    message = f"{success_count} out of {success_count + failure_count} tables had expected columns"

    return result, message, details


def duplicate_geometry_check(conn, spatial_field: str):
    """
    Compares all the geometries or points of entities in a dataset to find duplicates.
    Geometries are classed as duplicates if they have > 95% intersection,
    points are classed as duplicates if they are an exact match
    args:
        conn: spatialite connection used to connect to the db, wil be created by the checkpoint class
        spatial_field: the field to be used for comparison, either 'point' or 'geometry'
    """
    # Assuming spatialite connection so we don't have to install spatialite

    if spatial_field != "geometry" and spatial_field != "point":
        raise Exception(
            f"Spatial field for duplicate geometry check must be 'point' or 'geometry', not '{spatial_field}'"  # if we let people pass in spatial field this is required
        )

    # Create new table with spatial index on spatial field

    conn.execute("DROP TABLE IF EXISTS entity_spatial;")
    conn.execute(
        "SELECT InitSpatialMetadata(1);"
    )  # Initialise spatial metadata if it hasn't already, required to use AddGeometryColumn
    conn.execute(
        """
        CREATE TABLE entity_spatial (
            entity INTEGER,
            reference TEXT,
            organisation_entity INTEGER
        );
    """
    )
    # Add geometry column with SRID 0 (ie no co-ordinate reference system)
    if spatial_field == "geometry":
        conn.execute(
            "SELECT AddGeometryColumn('entity_spatial', 'geom', 0, 'GEOMETRY', 'XY');"
        )
        # Insert data into new table
        conn.execute(
            f"""
            INSERT INTO entity_spatial (entity, reference, organisation_entity, geom)
            SELECT entity, reference, organisation_entity, ST_GeomFromText({spatial_field}, 0)
            FROM entity
            WHERE {spatial_field} IS NOT NULL AND {spatial_field} != '';
        """
        )
        # Create the spatial index
        conn.execute("SELECT CreateSpatialIndex('entity_spatial', 'geom');")
    elif spatial_field == "point":
        conn.execute(
            "SELECT AddGeometryColumn('entity_spatial', 'point', 0, 'POINT', 'XY');"
        )
        # Insert data into new table
        conn.execute(
            f"""
            INSERT INTO entity_spatial (entity, reference, organisation_entity, point)
            SELECT entity, reference, organisation_entity, ST_PointFromText({spatial_field}, 0)
            FROM entity
            WHERE {spatial_field} IS NOT NULL AND {spatial_field} != '';
        """
        )

    # Now perform duplicate check using new table
    MATCH_THRESHOLD = 0.95
    if spatial_field == "geometry":
        query = f"""
            WITH calc as (
                SELECT
                    a.entity as entity_a,
                    a.organisation_entity as organisation_entity_a,
                    b.entity as entity_b,
                    b.organisation_entity as organisation_entity_b,
                    CAST(
                        MIN(a.entity, b.entity) AS TEXT
                    ) || '-' || CAST(
                        MAX(a.entity, b.entity) AS TEXT
                    ) AS entity_join_key,
                    ST_Area(ST_Intersection(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) as pct_comb_overlap,
                    ST_Area(ST_Intersection(a.geom, b.geom)) / ST_Area(a.geom) as pct_overlap_a,
                    ST_Area(ST_Intersection(a.geom, b.geom)) / ST_Area(b.geom) as pct_overlap_b
                FROM entity_spatial a
                JOIN entity_spatial b
                    ON ST_Intersects(a.geom, b.geom)
                    AND a.entity <> b.entity
                ),

            categorised as (

                SELECT
                    *,
                    CASE
                        WHEN pct_overlap_a > {MATCH_THRESHOLD} AND pct_overlap_b > {MATCH_THRESHOLD} THEN 'Complete match (two-way)'
                        WHEN pct_overlap_a > {MATCH_THRESHOLD} OR pct_overlap_b > {MATCH_THRESHOLD} THEN 'Single match (one-way)'
                    ELSE 'undefined' END as intersection_type,
                    row_number() OVER (PARTITION BY entity_join_key ORDER BY pct_comb_overlap) as key_count
                FROM calc
                WHERE pct_overlap_a > 0.9 OR pct_overlap_b > 0.9 -- should this use MATCH_THRESHOLD?
                ORDER BY entity_join_key
                )

            SELECT *
            FROM categorised
            WHERE key_count = 1
        """
    elif spatial_field == "point":
        query = """
            SELECT
                a.entity AS entity_a,
                a.organisation_entity as organisation_entity_a,
                b.entity AS entity_b,
                b.organisation_entity as organisation_entity_b,
                CAST(MIN(a.entity, b.entity) AS TEXT) || '-' || CAST(MAX(a.entity, b.entity) AS TEXT) AS entity_join_key
            FROM entity_spatial a
            JOIN entity_spatial b
                ON ST_Equals(a.point, b.point)
                AND a.entity <> b.entity
            GROUP BY entity_join_key;
        """

    conn.row_factory = sqlite3.Row
    rows = conn.execute(query).fetchall()

    rows = [dict(row) for row in rows]
    if len(rows) > 0:
        result = False
        message = f"There are {len(rows)} duplicate geometries/points in the dataset"
    else:
        result = True
        message = "There are no duplicate geometries/points in the dataset"

    details = {"actual": len(rows), "expected": 0, "entities": rows}

    return result, message, details
