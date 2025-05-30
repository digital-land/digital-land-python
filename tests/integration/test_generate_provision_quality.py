import pandas as pd
from unittest.mock import patch, Mock
from pathlib import Path
from datetime import datetime
from digital_land.commands import generate_provision_quality


@patch("digital_land.commands.duckdb.query")
@patch("digital_land.commands.fc.query_sqlite")
def test_generate_provision_quality(
    mock_query_sqlite,
    mock_duckdb_query,
):
    # mock issue_type
    df1 = pd.DataFrame(
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
        )
    # mock LPA boundary check
    df2 =pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "details": '{"actual": 2}',
                }
            ]
        )
    # mock count value
    df3 = pd.DataFrame(
            [
                {
                    "organisation": "org1",
                    "dataset": "dataset1",
                    "details": '{"actual": 1}',
                }
            ]
        )

    # Wrap each in a mock with .to_df()
    rel1 = Mock()
    rel1.to_df.return_value = df1

    rel2 = Mock()
    rel2.to_df.return_value = df2

    rel3 = Mock()
    rel3.to_df.return_value = df3

    mock_duckdb_query.side_effect = [rel1, rel2, rel3]

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
    assert not df.empty, "Dataframe loaded from Parquet is empty"
    assert set(["organisation", "dataset", "quality"]).issubset(df.columns)
    assert len(df) == 1
    assert df.iloc[0]["organisation"] == "org1"
    assert df.iloc[0]["dataset"] == "dataset1"
    assert df["quality"].iloc[0] in [
        "3. data that is good for ODP",
        "4. data that is trustworthy",
    ]
