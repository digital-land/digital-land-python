import json
import logging
import re
import sqlite3
import requests
import pandas as pd
import urllib
import os
import time
from collections import defaultdict
from datetime import datetime


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
        details = {"error": str(err)}
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
        SELECT entity, organisation_entity
        FROM entity
        WHERE (geometry != '' OR point != '')
    """

    if organisation_entity:
        query = query + f"AND organisation_entity = '{organisation_entity}'"

    query = query + f"AND ({spatial_condition});"
    rows = conn.execute(query).fetchall()
    entities = [row[0] for row in rows]
    actual = len(entities)
    # one failure per offending entity, carrying the organisation from the data rather
    # than the optional organisation_entity parameter, which not every config row supplies
    failures = [{"organisation_entity": row[1], "entity": row[0]} for row in rows]

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
        "failures": failures,
        "actual": actual,
        "expected": expected,
        "entities": entities,
    }

    return result, message, details


# Previously named count_deleted_entities() function
def fetch_active_resources_for_dataset(dataset_name):
    params = urllib.parse.urlencode(
        {
            "sql": f"""select o.entity as organisation_entity, rhe.resource
                        from reporting_historic_endpoints rhe
                        join organisation o on rhe.organisation=o.organisation
                        where pipeline == '{dataset_name}' and (resource_end_date == "" or resource_end_date is null)
                        group by o.entity, rhe.endpoint""",
            "_size": "max",
        }
    )
    base_url = f"https://datasette.planning.data.gov.uk/digital-land.csv?{params}"

    max_retries = 60
    for attempt in range(max_retries):
        try:
            df = pd.read_csv(base_url)
            logging.warning(
                f"[expectations] Fetched {len(df)} active resources for '{dataset_name}' from datasette"
            )
            cache = {}
            for _, row in df.iterrows():
                cache.setdefault(row["organisation_entity"], []).append(row["resource"])
            return cache
        except urllib.error.HTTPError as e:
            logging.warning(
                f"HTTP error fetching datasette for dataset {dataset_name}, "
                f"attempt {attempt + 1}/{max_retries}: {e}. Retrying in 60s..."
            )
            time.sleep(60)
    raise Exception(
        f"Failed to fetch datasette for dataset {dataset_name} after multiple attempts"
    )


def count_deleted_entities(
    conn,
    expected: int,
    organisation_entity: int = None,
    resources_cache: dict = None,
):
    # get database name to identify dataset
    db_path = conn.execute("PRAGMA database_list").fetchall()[0][2]
    db_name = os.path.splitext(os.path.basename(db_path))[0]

    # check if entity data has been fetched from datasette and stored in the cache
    if resources_cache is not None:
        resource_list = resources_cache.get(organisation_entity, [])
    else:
        # get dataset specific active resource list if nothing is found it
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
            except urllib.error.HTTPError as e:
                logging.warning(
                    f"HTTP error fetching datasette for organisation {organisation_entity}, "
                    f"attempt {attempt + 1}/{max_retries}: {e}. Retrying in 60s..."
                )
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
    failures = (
        [{"organisation_entity": str(organisation_entity), "count": actual}]
        if actual
        else []
    )

    result = bool(actual == expected)
    message = f"there were {actual} entities found"
    details = {
        "failures": failures,
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
        conn.execute("SELECT CreateSpatialIndex('entity_spatial', 'point');")

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
                    AND a.entity < b.entity
                ),

            categorised as (

                SELECT
                    *,
                    CASE
                        WHEN pct_overlap_a > {MATCH_THRESHOLD} AND pct_overlap_b > {MATCH_THRESHOLD} THEN 'Complete match (two-way)'
                        WHEN pct_overlap_a > {MATCH_THRESHOLD} OR pct_overlap_b > {MATCH_THRESHOLD} THEN 'Single match (one-way)'
                    ELSE 'Any match' END as intersection_type,
                    row_number() OVER (PARTITION BY entity_join_key ORDER BY pct_comb_overlap) as key_count
                FROM calc
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
                AND a.entity < b.entity
            GROUP BY entity_join_key;
        """

    conn.row_factory = sqlite3.Row
    rows = conn.execute(query).fetchall()

    rows = [dict(row) for row in rows]
    if len(rows) > 0:
        result = False
        if spatial_field == "geometry":
            complete_matches = [
                {
                    "entity_a": row["entity_a"],
                    "organisation_entity_a": row["organisation_entity_a"],
                    "entity_b": row["entity_b"],
                    "organisation_entity_b": row["organisation_entity_b"],
                }
                for row in rows
                if row["intersection_type"] == "Complete match (two-way)"
            ]

            single_matches = [
                {
                    "entity_a": row["entity_a"],
                    "organisation_entity_a": row["organisation_entity_a"],
                    "entity_b": row["entity_b"],
                    "organisation_entity_b": row["organisation_entity_b"],
                }
                for row in rows
                if row["intersection_type"] == "Single match (one-way)"
            ]

            any_matches = [
                {
                    "entity_a": row["entity_a"],
                    "organisation_entity_a": row["organisation_entity_a"],
                    "entity_b": row["entity_b"],
                    "organisation_entity_b": row["organisation_entity_b"],
                }
                for row in rows
                if row["intersection_type"] == "Any match"
            ]
            message = f"There are {len(complete_matches)} complete matches, {len(single_matches)} single matches and {len(any_matches)} any matches in the dataset"
        else:
            complete_matches = [
                {
                    "entity_a": row["entity_a"],
                    "organisation_entity_a": row["organisation_entity_a"],
                    "entity_b": row["entity_b"],
                    "organisation_entity_b": row["organisation_entity_b"],
                }
                for row in rows
            ]
            single_matches = []
            any_matches = []
            message = (
                f"There are {len(complete_matches)} complete matches in the dataset"
            )
    else:
        result = True
        message = "There are no duplicate geometries/points in the dataset"
        complete_matches = []
        single_matches = []
        any_matches = []

    match_counts = defaultdict(
        lambda: {"complete_matches": 0, "single_matches": 0, "any_matches": 0}
    )
    for match_type, matches in (
        ("complete_matches", complete_matches),
        ("single_matches", single_matches),
        ("any_matches", any_matches),
    ):
        for match in matches:
            for org in {
                match["organisation_entity_a"],
                match["organisation_entity_b"],
            }:
                if org in (None, ""):
                    continue
                match_counts[org][match_type] += 1

    # any_matches are polygons that merely intersect, neither covering 95% of the other -
    # adjacency, not duplication. an organisation with only those has nothing to act on.
    failures = [
        {
            "organisation_entity": str(org),
            "complete_matches": counts["complete_matches"],
            "single_matches": counts["single_matches"],
            "any_matches": counts["any_matches"],
            "count": counts["complete_matches"] + counts["single_matches"],
        }
        for org, counts in match_counts.items()
        if counts["complete_matches"] + counts["single_matches"] > 0
    ]
    failures.sort(
        key=lambda failure: (-failure["count"], failure["organisation_entity"])
    )

    # a dataset with only any_matches comes back passed=False with failures=[]: it has
    # overlaps, but none of them actionable.
    details = {
        "failures": failures,
        "actual": len(rows),
        "expected": 0,
        "complete_matches": complete_matches,
        "single_matches": single_matches,
        "any_matches": any_matches,
    }
    return result, message, details


# Flags a name which contains at least one digit, is 20 characters or fewer, and is
# made up only of letters, digits, dots, slashes and hyphens - a bare reference code
# rather than a description. The digit lookahead is what stops plain single-word place
# names such as 'Napsbury' being flagged.
CODE_LIKE_NAME_RE = re.compile(r"^(?=.*[0-9])[A-Za-z0-9./-]{1,20}$")


def name_is_a_code_check(conn):
    """
    Checks for entities whose name is just a bare reference code rather than a
    description, e.g. '59', '0164B2' or '11/802'.

    Failures carry organisation_entity rather than an organisation curie. The dataset
    package deliberately keeps 'organisation' out of the entity json (package/dataset.py),
    so organisation_entity is the only provenance available here - resolving it to a
    curie is the bridge's job.

    Blank names are not flagged here - they are already covered by the existing
    missing values issue, and the pattern cannot match an empty string anyway.

    args:
        conn: connection to the dataset being checked, created by the checkpoint class
    """
    # length is a cheap superset of the pattern's {1,20}, so this can only ever exclude
    # rows the pattern would have rejected, and it keeps the regex off most of the table
    query = """
        select entity, reference, name, organisation_entity
        from entity
        where name is not null
            and trim(name) != ''
            and length(name) <= 20
    """
    rows = conn.execute(query).fetchall()

    failures = [
        {
            "organisation_entity": organisation_entity,
            "entity": entity,
            "reference": reference,
            "name": name,
        }
        for entity, reference, name, organisation_entity in rows
        if CODE_LIKE_NAME_RE.match(name)
    ]

    failures.sort(
        key=lambda failure: (
            failure["organisation_entity"] or "",
            failure["reference"] or "",
        )
    )

    result = len(failures) == 0
    message = f"{len(failures)} entities have a name which is only a reference code"
    details = {
        "failures": failures,
        "field": "name",
        "actual": len(failures),
        "expected": 0,
    }

    return result, message, details


# Placeholder strings that some source systems write into `name` when they have no
# real name, e.g. "No name for this Entry". Overridable per-dataset from the
# parameters cell in config's expect.csv, so new boilerplate spotted in the data can
# be added without a code release.
DEFAULT_PLACEHOLDER_NAMES = [
    "No name for this Entry",
]


def _normalise_name(name):
    """casefold and collapse whitespace so matching ignores case and spacing"""
    return re.sub(r"\s+", " ", name).strip().casefold()


def name_is_a_placeholder_check(conn, listed_building_path, placeholders=None):
    """
    Checks for entities whose name is a placeholder string, e.g. 'No name for this
    Entry', where the linked listed-building record holds a real name instead.

    Matching is exact after normalisation - deliberately NOT a substring match, so a
    real name which happens to contain the word 'name' is not flagged.

    `name` is a mandatory field for listed-building-outline (MANDATORY_FIELDS_DICT in
    digital_land/phase/harmonise.py), so a blank name already raises a missing value
    issue.

    args:
        conn: connection to the dataset being checked, created by the checkpoint class
        listed_building_path: path to a local copy of the built listed-building sqlite,
            fetched as a published artifact during assemble
        placeholders: list of placeholder strings; falls back to
            DEFAULT_PLACEHOLDER_NAMES when absent or empty, so a parameters cell
            carrying only the path still works
    """
    if not placeholders:
        placeholders = DEFAULT_PLACEHOLDER_NAMES
    # a single string in config would otherwise iterate as characters and match any
    # one-character name, so treat it as one placeholder
    if isinstance(placeholders, str):
        placeholders = [placeholders]

    placeholder_set = {_normalise_name(placeholder) for placeholder in placeholders}

    # a missing artifact must never fail the build, the check just can't run
    if not listed_building_path or not os.path.isfile(listed_building_path):
        message = (
            f"listed-building not available at '{listed_building_path}', check skipped"
        )
        logging.warning(message)
        return True, message, {"skipped": True, "failures": []}

    # listed-building is not a core entity column, so it lives in the json blob.
    query = """
        select
            e.entity,
            e.reference,
            e.name,
            e.organisation_entity,
            lb.name as listed_building_name
        from entity e
        inner join listed_building.entity lb
            on lb.reference = json_extract(e.json, '$."listed-building"')
        where e.name is not null
            and trim(e.name) != ''
            and lb.name is not null
            and trim(lb.name) != ''
    """

    conn.execute("ATTACH DATABASE ? AS listed_building", (str(listed_building_path),))
    try:
        rows = conn.execute(query).fetchall()
    finally:
        conn.execute("DETACH DATABASE listed_building")

    failures = [
        {
            "organisation_entity": organisation_entity,
            "entity": entity,
            "reference": reference,
            "name": name,
            "listed_building_name": listed_building_name,
        }
        for entity, reference, name, organisation_entity, listed_building_name in rows
        # the listed-building name must be a real name too - swapping one placeholder
        # for another is not a fix worth asking an organisation to make
        if _normalise_name(name) in placeholder_set
        and _normalise_name(listed_building_name) not in placeholder_set
    ]

    failures.sort(
        key=lambda failure: (
            failure["organisation_entity"] or "",
            failure["reference"] or "",
            failure["entity"],
        )
    )

    result = len(failures) == 0
    message = (
        f"{len(failures)} entities have a placeholder name where the listed building "
        "record holds a real name"
    )
    details = {
        "failures": failures,
        "field": "name",
        "actual": len(failures),
        "expected": 0,
    }

    return result, message, details


def duplicate_name_check(conn, threshold=2):
    """
    Checks for names reused across several entities within the same provision, i.e.
    the same dataset and organisation. Usually means one feature was split into
    several polygons and the parts were never distinguished.

    Grouping is per organisation, not dataset-wide. The published dataset is every
    LPA's file concatenated together, so a dataset-wide comparison flags authorities
    for other authorities' word choices - 'City Centre' appears once each in seven
    different councils, and none of them can act on that.

    Failures are one entry per duplicated name rather than per entity, matching the
    analysis this came from. Member entity ids are nested under 'entities' so a
    drill-down page can page through them without re-running the check.

    Matching is case- and whitespace-insensitive via _normalise_name, but not
    otherwise fuzzy: two directions differing only by year are different names.

    Blank names are not compared - they are already covered by the missing values
    issue, and every blank would otherwise be a duplicate of every other blank.

    args:
        conn: connection to the dataset being checked, created by the checkpoint class
        threshold: how many entities must share a name before it is reported.
            Exposed so it can be raised from config without a code release.
    """
    query = """
        select entity, reference, name, organisation_entity, dataset
        from entity
        where name is not null
            and trim(name) != ''
        order by entity
    """
    rows = conn.execute(query).fetchall()

    groups = {}
    for entity, reference, name, organisation_entity, dataset in rows:
        normalised = _normalise_name(name)
        # sqlite's trim() only strips spaces, so a tab- or newline-only name survives
        # the query. left in, every such row would group together under an empty key.
        if not normalised:
            continue
        key = (dataset, organisation_entity, normalised)
        # keep the raw spelling of the lowest entity id: the normalised key is not worth
        # showing to an LPA, and two rows differing only in case would otherwise look
        # identical. the order by makes which spelling wins stable across rebuilds.
        group = groups.setdefault(key, {"name": name, "entities": [], "references": []})
        group["entities"].append(entity)
        group["references"].append(reference)

    failures = [
        {
            "organisation_entity": organisation_entity,
            "name": group["name"],
            "count": len(group["entities"]),
            "entities": sorted(group["entities"]),
            "references": sorted(
                reference for reference in group["references"] if reference
            ),
        }
        for (_, organisation_entity, _), group in groups.items()
        if len(group["entities"]) >= threshold
    ]

    failures.sort(
        key=lambda failure: (
            failure["organisation_entity"] or "",
            -failure["count"],
            failure["name"],
        )
    )

    result = len(failures) == 0
    message = f"{len(failures)} names are used by {threshold} or more entities"
    details = {
        "failures": failures,
        "field": "name",
        "actual": len(failures),
        "expected": 0,
    }

    return result, message, details


def check_fields_required_after_plan_event(
    conn,
    fields: list,
    plan_timetable_path: str,
    plan_event: str = "proposed-plan-consultation-start",
    today: str = None,
):
    """
    Checks that a set of fields are populated on plan records, but only for plans
    which have reached a given stage of plan making.

    A plan reaches the stage when its linked plan-timetable row for plan_event has
    an actual-date in the past. Plans which have not reached that stage are ignored,
    so no issue is raised for data the LPA is not yet expected to supply.

    The plan-timetable data lives in a different dataset, so it is fetched as a
    published artifact during assemble and attached here as a second database.

    args:
        conn: connection to the dataset being checked, created by the checkpoint class
        fields: the fields which must be populated once the stage is reached
        plan_timetable_path: path to a local copy of the built plan-timetable sqlite
        plan_event: the plan-timetable event which opens the gate
        today: date to compare against as YYYY-MM-DD, defaults to the current date
    """
    if not fields:
        raise ValueError("At least one field must be provided to check")

    today = today or datetime.now().strftime("%Y-%m-%d")

    # a missing timetable artifact must never fail the build, the check just can't run
    if not plan_timetable_path or not os.path.isfile(plan_timetable_path):
        message = (
            f"plan-timetable not available at '{plan_timetable_path}', check skipped"
        )
        logging.warning(message)
        return True, message, {"skipped": True, "failures": []}

    # a plan can have more than one row for the same event, so take the earliest
    # actual-date, the stage is reached as soon as any of them has passed
    query = """
        select
            e.reference,
            e.json,
            e.organisation_entity,
            min(json_extract(t.json, '$."actual-date"')) as actual_date
        from entity e
        inner join plan_timetable.entity t
            on json_extract(t.json, '$.plan') = e.reference
        where json_extract(t.json, '$."plan-event"') = ?
            and coalesce(json_extract(t.json, '$."actual-date"'), '') != ''
            and json_extract(t.json, '$."actual-date"') < ?
        group by e.entity
    """

    conn.execute("ATTACH DATABASE ? AS plan_timetable", (str(plan_timetable_path),))
    try:
        rows = conn.execute(query, (plan_event, today)).fetchall()
    finally:
        conn.execute("DETACH DATABASE plan_timetable")

    failures = []
    for reference, entity_json, organisation_entity, actual_date in rows:
        entity_fields = json.loads(entity_json or "{}")
        for field in fields:
            value = entity_fields.get(field)
            if value is not None and str(value).strip():
                continue
            failures.append(
                {
                    "organisation": entity_fields.get("organisations") or "",
                    "organisation_entity": organisation_entity,
                    "reference": reference,
                    "field": field,
                    "actual-date": actual_date,
                }
            )

    failures.sort(key=lambda failure: (failure["reference"], failure["field"]))

    result = len(failures) == 0
    message = (
        f"{len(failures)} missing values found across {len(rows)} plans "
        f"which have passed {plan_event}"
    )
    details = {
        "failures": failures,
        "actual": len(failures),
        "expected": 0,
        "fields": fields,
        "plan_event": plan_event,
        "plans_past_event": len(rows),
    }

    return result, message, details
