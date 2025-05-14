import os
import pandas as pd
import numpy as np
import json
from datetime import datetime
from digital_land.utils import functions_core as fc


def generate_provision_quality():
    """
    Generates a provision quality dataset and saves it as a parquet file.
    """
    td = datetime.today().strftime("%Y-%m-%d")

    # Create the temporary download directory
    db_dir = os.path.join("/tmp", "db_downloads")
    os.makedirs(db_dir, exist_ok=True)

    # Download the performance db
    fc.download_dataset("performance", db_dir, overwrite=False)
    path_perf_db = os.path.join(db_dir, "performance.db")

    # Issue quality criteria lookup
    lookup_issue_qual = fc.datasette_query(
        "digital-land",
        """
        SELECT
            description,
            issue_type,
            name,
            severity,
            responsibility,
            quality_criteria_level || " - " || quality_criteria as quality_criteria,
            quality_criteria_level as quality_level
        FROM issue_type
        WHERE quality_criteria_level != ''
        AND quality_criteria != ''
        """,
    )

    # Transform data
    provision = fc.query_sqlite(
        path_perf_db,
        """
        SELECT organisation, dataset, active_endpoint_count
        FROM provision_summary
        """,
    )

    # Extract issue count by provision from endpoint_dataset_issue_type_summary
    qual_issue = fc.query_sqlite(
        path_perf_db,
        """
        SELECT
            organisation, dataset,
            'issue' as problem_source,
            issue_type as problem_type,
            sum(count_issues) as count
        FROM endpoint_dataset_issue_type_summary
        WHERE resource_end_date is not NULL
        AND issue_type is not NULL
        GROUP BY organisation, dataset, issue_type
        """,
    )

    # Join on quality criteria and level from issue_type lookup (this restricts to only issues linked to a quality criteria)
    qual_issue = qual_issue.merge(
        lookup_issue_qual[["issue_type", "quality_criteria", "quality_level"]],
        how="inner",
        left_on="problem_type",
        right_on="issue_type",
    )
    qual_issue.drop("issue_type", axis=1, inplace=True)

    # IDENTIFY PROBLEMS - expectations - entity beyond LPA bounds
    qual_expectation_bounds = fc.datasette_query(
        "digital-land",
        """
        SELECT organisation, dataset, details
        FROM expectation
        WHERE 1=1
            AND name = 'Check no entities are outside of the local planning authority boundary'
            AND passed = 'False'
            AND message not like '%error%'
        """,
    )

    qual_expectation_bounds["problem_source"] = "expectation"
    qual_expectation_bounds["problem_type"] = (
        "entity outside of the local planning authority boundary"
    )
    qual_expectation_bounds["count"] = [
        json.loads(v)["actual"] for v in qual_expectation_bounds["details"]
    ]
    qual_expectation_bounds["quality_criteria"] = "3 - entities within LPA boundary"
    qual_expectation_bounds["quality_level"] = 3
    qual_expectation_bounds.drop("details", axis=1, inplace=True)

    # IDENTIFY PROBLEMS - expectations - entity beyond LPA bounds
    qual_expectation_count = fc.datasette_query(
        "digital-land",
        """
        SELECT organisation, dataset, details
        FROM expectation
        WHERE 1=1
            AND name = 'Check number of entities inside the local planning authority boundary matches the manual count'
            AND passed = 'False'
            AND message not like '%error%'
        """,
    )

    qual_expectation_count["problem_source"] = "expectation"
    qual_expectation_count["problem_type"] = "entity count doesn't match manual count"
    qual_expectation_count["count"] = [
        json.loads(v)["actual"] for v in qual_expectation_count["details"]
    ]
    qual_expectation_count["quality_criteria"] = (
        "3 - conservation area entity count matches LPA"
    )
    qual_expectation_count["quality_level"] = 3
    qual_expectation_count.drop("details", axis=1, inplace=True)

    # Combine all problem source tables, and aggregate to criteria level
    qual_all_criteria = (
        pd.concat([qual_issue, qual_expectation_bounds, qual_expectation_count])
        .groupby(
            ["organisation", "dataset", "quality_criteria", "quality_level"],
            as_index=False,
        )
        .agg(count_failures=("count", "sum"))
    )

    # Merge issues with the provision data
    prov_qual_all = provision.merge(
        qual_all_criteria, how="left", on=["organisation", "dataset"]
    )

    prov_qual_all["quality_level_for_sort"] = np.select(
        [
            (prov_qual_all["active_endpoint_count"] == 0),
            (prov_qual_all["quality_level"].notnull()),
            (prov_qual_all["active_endpoint_count"] > 0)
            & (prov_qual_all["quality_level"].isnull()),
        ],
        [0, prov_qual_all["quality_level"], 4],
    )

    level_map = {
        4: "4. data that is trustworthy",
        3: "3. data that is good for ODP",
        2: "2. authoritative data from the LPA",
        1: "1. some data",
        0: "0. no score",
    }

    prov_quality = prov_qual_all.groupby(
        ["organisation", "dataset"], as_index=False, dropna=False
    ).agg(quality_level=("quality_level_for_sort", "min"))

    prov_quality["quality"] = prov_quality["quality_level"].map(level_map)
    prov_quality["notes"] = ""
    prov_quality["end-date"] = ""
    prov_quality["start-date"] = td
    prov_quality["entry-date"] = td

    # Output the results as a Parquet file
    output_dir = os.path.join(
        "/tmp", "performance", "provision-quality", f"entry-date={td}"
    )
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "provision-quality.parquet")
    prov_quality.to_parquet(output_file, engine="pyarrow", index=False)

    print(f"Provision quality dataset saved to: {output_file}")
