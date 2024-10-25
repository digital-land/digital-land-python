import csv
import os
import pytest
import duckdb

from digital_land.commands import convert_csvs_to_parquet


@pytest.fixture(scope="session")
def issue_dir(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")

    issue_paths = []
    issue_paths.append(os.path.join(issue_dir, "dir/dataset1/resource1.csv"))
    issue_paths.append(os.path.join(issue_dir, "dir/dataset2/resource2.csv"))

    field_names = ["header1", "header2", "header3"]
    rows = [{"header1": "value1", "header2": "value2", "header3": "value3"}]
    for issue_path in issue_paths:
        os.makedirs(os.path.dirname(issue_path), exist_ok=True)
        with open(issue_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(rows)

    return issue_dir


def test_convert_csvs_to_parquet(tmp_path_factory, issue_dir):
    output_dir = tmp_path_factory.mktemp("parquet_issues")
    convert_csvs_to_parquet(issue_dir, output_dir)
    assert os.path.isfile(os.path.join(output_dir, "dir/dataset1/resource1.parquet"))
    assert os.path.isfile(os.path.join(output_dir, "dir/dataset2/resource2.parquet"))
    conn = duckdb.connect()
    parquet_path = os.path.join(output_dir, "dir/dataset1/resource1.parquet")
    df = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
    assert (set(df.columns) - set(["header1", "header2", "header3"])) == set()
    assert (set(df.iloc[0].values) - set(["value1", "value2", "value3"])) == set()
