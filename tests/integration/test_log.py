import os

import duckdb
from digital_land.log import Log


def test_log_save_parquet(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "resource"
    fieldnames = ["dataset", "resource", "issue", "entry-number"]

    log = Log()
    log.dataset = dataset
    log.resource = resource
    log.fieldnames = fieldnames

    log.rows = [
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue1",
            "entry-number": 1,
        },
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue2",
            "entry-number": 1,
        },
    ]

    output_dir = tmp_path_factory.mktemp("output")

    log.save_parquet(output_dir)

    parquet_path = os.path.join(
        output_dir, f"dataset={dataset}/resource={resource}/{resource}.parquet"
    )
    assert os.path.isfile(parquet_path)
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
    assert (set(df.iloc[0].values) - set([dataset, resource, "issue1", 1])) == set()
    assert (set(df.columns) - set(fieldnames)) == set()


def test_log_save_parquet_no_rows(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "norows"
    fieldnames = ["dataset", "resource", "issue"]

    log = Log()
    log.dataset = dataset
    log.resource = resource
    log.fieldnames = fieldnames

    output_dir = tmp_path_factory.mktemp("parquet_no_rows")

    log.save_parquet(output_dir)

    parquet_path = os.path.join(
        output_dir, f"dataset={dataset}/resource={resource}/{resource}.parquet"
    )
    assert os.path.isfile(parquet_path)
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
    assert (set(df.columns) - set(fieldnames)) == set()


def test_log_save_parquet_large_int(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "resource"
    fieldnames = ["dataset", "resource", "issue", "entry-number"]

    log = Log()
    log.dataset = dataset
    log.resource = resource
    log.fieldnames = fieldnames

    log.rows = [
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue1",
            "entry-number": 33281,
        },
    ]

    output_dir = tmp_path_factory.mktemp("output")

    log.save_parquet(output_dir)

    parquet_path = os.path.join(
        output_dir, f"dataset={dataset}/resource={resource}/{resource}.parquet"
    )
    assert os.path.isfile(parquet_path)
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
    assert (set(df.iloc[0].values) - set([dataset, resource, "issue1", 33281])) == set()
    assert (set(df.columns) - set(fieldnames)) == set()


def test_log_save_parquet_non_string(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "resource"
    fieldnames = ["dataset", "resource", "issue", "value"]

    log = Log()
    log.dataset = dataset
    log.resource = resource
    log.fieldnames = fieldnames

    log.rows = [
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue1",
            "value": 1,
        },
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue2",
            "value": ["list", "here"],
        },
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue3",
            "value": 1.2,
        },
        {
            "dataset": dataset,
            "resource": resource,
            "issue": "issue4",
            "value": {"python": "dict"},
        },
    ]

    output_dir = tmp_path_factory.mktemp("output")

    log.save_parquet(output_dir)

    parquet_path = os.path.join(
        output_dir, f"dataset={dataset}/resource={resource}/{resource}.parquet"
    )
    assert os.path.isfile(parquet_path)
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM '{parquet_path}'").df()
    print(df.iloc[1].values)
    assert (set(df.iloc[0].values) - set([dataset, resource, "issue1", "1"])) == set()
    assert (
        set(df.iloc[1].values) - set([dataset, resource, "issue2", "['list', 'here']"])
    ) == set()
    assert (set(df.iloc[2].values) - set([dataset, resource, "issue3", "1.2"])) == set()
    assert (
        set(df.iloc[3].values)
        - set([dataset, resource, "issue4", "{'python': 'dict'}"])
    ) == set()
    assert (set(df.columns) - set(fieldnames)) == set()
