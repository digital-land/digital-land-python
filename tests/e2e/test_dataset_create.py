from click.testing import CliRunner
import csv
import os
import urllib.request
import pandas as pd
import sqlite3

import pytest

from digital_land.cli import dataset_create_cmd
from digital_land.pipeline import Pipeline


@pytest.fixture(scope="session")
def pipeline_dir(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    return pipeline_dir


@pytest.fixture(scope="session")
def organisation_path(tmp_path_factory):
    organisation_dir = tmp_path_factory.mktemp("organisation")

    organisation_path = organisation_dir / "organisation.csv"
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )

    return organisation_path


@pytest.fixture(scope="session")
def transformed_file_path(tmp_path_factory):
    transformed_dir = tmp_path_factory.mktemp("transformed")
    resource_hash = "435138a11d3c50f7ff217c8e938b44376ef5938543702b0bd7c06cd3a4e7422c"
    out_file_path = transformed_dir / f"{resource_hash}.csv"

    # create transformed file source
    row = {
        "end-date": "",
        "entity": "700000",
        "entry-date": "2023-10-03",
        "entry-number": "1",
        "fact": "7fa1bd3c802494f4998599b80c828146f621987da954d3490a4b98a5f24a8b4d",
        "field": "entry-date",
        "reference-entity": "",
        "resource": "47e8c774370b10c803da048f4f1d98cfb028d25e01408314c92f0ac74cbee12b",
        "start-date": "",
        "value": "2023-10-03",
    }
    fieldnames = row.keys()

    with open(out_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    return str(out_file_path)


@pytest.mark.parametrize("run_condition", ["no_max_batch_size", "max_batch_size"])
def test_dataset_create_with_max_batch_size(
    organisation_path, transformed_file_path, pipeline_dir, tmp_path, run_condition
):
    dataset = "listed-building"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    run_args = [
        transformed_file_path,
        "--organisation-path",
        organisation_path,
        "--output-path",
        sqlite3_path,
    ]

    if run_condition == "max_batch_size":
        run_args.append("--max-batch-size")
        run_args.append(10000)

    runner = CliRunner()
    result = runner.invoke(
        dataset_create_cmd,
        args=run_args,
        obj={
            "PIPELINE": Pipeline(pipeline_dir, dataset),
            "DATASET": dataset,
            "SPECIFICATION": None,
        },
    )

    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT
            (select count(*) from fact) as f_count,
            (select count(*) from fact_resource) as fr_count,
            (select count(*) from column_field) as cf_count,
            (select count(*) from dataset_resource) as dr_count,
            (select count(*) from entity) as e_count,
            (select count(*) from issue) as i_count,
            (select count(*) from old_entity) as oe_count;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        qry_result = cursor.fetchall()
        results_df = pd.DataFrame.from_records(data=qry_result, columns=cols)

        assert results_df["f_count"].values[0] == 1
        assert results_df["fr_count"].values[0] == 1
        assert results_df["cf_count"].values[0] == 1
        assert results_df["dr_count"].values[0] == 1
        assert results_df["e_count"].values[0] == 1
        assert results_df["i_count"].values[0] == 0
        assert results_df["oe_count"].values[0] == 0

    assert result.exit_code == 0, f"{result.stdout}"
