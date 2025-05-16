import pandas as pd
from unittest.mock import patch
from pathlib import Path
from datetime import datetime
from digital_land.commands import generate_provision_quality


@patch("digital_land.commands.fc.datasette_query")
@patch("digital_land.commands.fc.query_sqlite")
def test_generate_provision_quality(
    mock_query_sqlite,
    mock_datasette_query,
):
    # mock issue_type
    mock_datasette_query.side_effect = [
        pd.DataFrame(
            [
                {
                    "description": "desc",
                    "issue_type": "missing-value",
                    "name": "Missing Value",
                    "severity": "error",
                    "responsibility": "external",
                    "quality_criteria": "any other validity error",
                    "quality_level": 3,
                }
            ]
        ),
        # mock LPA boundary check
        pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "details": '{"actual": 2}',
                }
            ]
        ),
        # mock count value
        pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "details": '{"actual": 1}',
                }
            ]
        ),
    ]

    # mock sqlite queries
    mock_query_sqlite.side_effect = [
        pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "active_endpoint_count": 5,
                }
            ]
        ),
        pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "problem_source": "issue",
                    "problem_type": "missing-value",
                    "count": 1,
                }
            ]
        ),
    ]

    generate_provision_quality()

    td = datetime.today().strftime("%Y-%m-%d")
    output_file = Path(
        f"/tmp/performance/provision-quality/entry-date={td}/provision-quality.parquet"
    )
    assert output_file.exists(), "Parquet file not found"

    df = pd.read_parquet(output_file)
    assert "organisation" in df.columns
    assert "dataset" in df.columns
    assert "quality" in df.columns

    assert not df.empty, "Dataframe loaded from Parquet is empty"
    assert len(df) == 1
    assert df.iloc[0]["organisation"] == "org1"
