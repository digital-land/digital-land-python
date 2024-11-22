import sqlite3
import numpy as np
import pandas as pd
import pytest
import os
from digital_land.package.datasetparquet import DatasetParquetPackage


# Fixture to create a shared temporary directory
@pytest.fixture(scope="session")
def temp_dir(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("shared_session_temp_dir")
    yield temp_dir


@pytest.fixture
def test_dataset_parquet_package(temp_dir):
    # Set up temporary files with mock data
    input_paths = [
        temp_dir / "hash1.csv",
        temp_dir / "hash2.csv",
        temp_dir / "hash3.csv",
    ]
    columns = [
        "end-date",
        "entity",
        "entry-date",
        "entry-number",
        "fact",
        "field",
        "priority",
        "reference-entity",
        "resource",
        "start-date",
        "value",
    ]
    # Test data for the tables. This checks that 'field' get pivoted
    test_geometry = "MULTIPOLYGON(((-0.49901924 53.81622,-0.5177418 53.76114,-0.4268378 53.78454,-0.49901924 53.81622)))"
    data = [
        [
            np.nan,
            11,
            "2023-01-01",
            2,
            "abcdef",
            "entry-date",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            11,
            "2023-01-01",
            2,
            "abcdef",
            "geometry",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            f'"{test_geometry}"',
        ],
        [
            np.nan,
            11,
            "2023-01-01",
            2,
            "abcdef",
            "organisation",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            "local-authority:AAA",
        ],
        [
            np.nan,
            12,
            "2023-02-01",
            2,
            "abc123",
            "entry-date",
            2,
            np.nan,
            "yxwvut",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            12,
            "2023-02-01",
            2,
            "abc123",
            "geometry",
            2,
            np.nan,
            "yxwvut",
            np.nan,
            f'"{test_geometry}"',
        ],
        [
            np.nan,
            12,
            "2023-01-01",
            2,
            "abc123",
            "organisation",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            "local-authority:BBB",
        ],
        [
            np.nan,
            13,
            "2023-01-01",
            2,
            "def456",
            "entry-date",
            2,
            np.nan,
            "xwvuts",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            13,
            "2023-01-01",
            2,
            "def456",
            "geometry",
            2,
            np.nan,
            "xwvuts",
            np.nan,
            f'"{test_geometry}"',
        ],
        [
            np.nan,
            13,
            "2023-01-01",
            2,
            "def456",
            "organisation",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            "local-authority:CCC",
        ],
        [
            np.nan,
            14,
            "2023-01-01",
            2,
            "a1b2c3",
            "entry-date",
            2,
            np.nan,
            "wvutsr",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            14,
            "2023-01-01",
            2,
            "a1b2c3",
            "geometry",
            2,
            np.nan,
            "wvutsr",
            np.nan,
            f'"{test_geometry}"',
        ],
        [
            np.nan,
            14,
            "2023-01-01",
            2,
            "a1b2c3",
            "organisation",
            2,
            np.nan,
            "zyxwvu",
            np.nan,
            "local-authority:DDD",
        ],
    ]
    with open(input_paths[0], "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(",".join(map(str, row)) + "\n")

    # df = pd.read_csv(input_paths[0])

    # Test data for the tables. This has plenty of 'duplicates' to check
    data = [
        [
            np.nan,
            110,
            "2023-01-01",
            2,
            "badcfe",
            "entry-date",
            2,
            np.nan,
            "zyx123",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            110,
            "2023-01-01",
            2,
            "badcfe",
            "entry-date",
            2,
            np.nan,
            "zyx123",
            np.nan,
            "2023-01-01",
        ],  # same
        [
            np.nan,
            110,
            "2023-01-01",
            2,
            "badcfe",
            "organisation",
            2,
            np.nan,
            "zyx123",
            np.nan,
            "local-authority:DDD",
        ],
        [
            np.nan,
            111,
            "2023-01-01",
            2,
            "fedcba",
            "entry-date",
            2,
            np.nan,
            "zxy123",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            111,
            "2023-02-01",
            2,
            "fedcba",
            "entry-date",
            2,
            np.nan,
            "zxy123",
            np.nan,
            "2023-02-01",
        ],  # ent-date
        [
            np.nan,
            111,
            "2023-02-01",
            2,
            "fedcba",
            "organisation",
            2,
            np.nan,
            "zxy123",
            np.nan,
            "local-authority:EEE",
        ],
        [
            np.nan,
            112,
            "2023-02-01",
            2,
            "bcdefg",
            "entry-date",
            2,
            np.nan,
            "yxw456",
            np.nan,
            "2023-02-01",
        ],
        [
            np.nan,
            112,
            "2023-02-01",
            12,
            "bcdefg",
            "entry-date",
            2,
            np.nan,
            "yxw456",
            np.nan,
            "2023-02-01",
        ],  # ent-no
        [
            np.nan,
            112,
            "2023-01-01",
            12,
            "bcdefg",
            "organisation",
            2,
            np.nan,
            "yxw456",
            np.nan,
            "local-authority:FFF",
        ],
        [
            np.nan,
            113,
            "2023-01-01",
            2,
            "cdefgh",
            "entry-date",
            2,
            np.nan,
            "xwv789",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            113,
            "2023-01-01",
            2,
            "hgfedc",
            "entry-date",
            2,
            np.nan,
            "xwv789",
            np.nan,
            "2023-01-01",
        ],  # fact
        [
            np.nan,
            113,
            "2023-01-01",
            2,
            "cdefgh",
            "organisation",
            2,
            np.nan,
            "xwv789",
            np.nan,
            "local-authority:GGG",
        ],
        [
            np.nan,
            114,
            "2023-04-01",
            2,
            "efghij",
            "entry-date",
            1,
            np.nan,
            "xyz123",
            np.nan,
            "2023-04-01",
        ],
        [
            np.nan,
            114,
            "2023-04-01",
            2,
            "efghij",
            "entry-date",
            2,
            np.nan,
            "xyz123",
            np.nan,
            "2023-04-01",
        ],  # priority
        [
            np.nan,
            114,
            "2023-01-01",
            2,
            "efghij",
            "organisation",
            1,
            np.nan,
            "xyz123",
            np.nan,
            "local-authority:HHH",
        ],
        [
            np.nan,
            115,
            "2023-01-01",
            2,
            "defghi",
            "entry-date",
            2,
            np.nan,
            "uvw456",
            np.nan,
            "2023-01-01",
        ],
        [
            np.nan,
            115,
            "2023-01-01",
            2,
            "defghi",
            "entry-date",
            2,
            np.nan,
            "wvu654",
            np.nan,
            "2023-01-01",
        ],  # resource
        [
            np.nan,
            115,
            "2023-01-01",
            2,
            "defghi",
            "organisation",
            2,
            np.nan,
            "uvw456",
            np.nan,
            "local-authority:III",
        ],
        [
            np.nan,
            116,
            "2023-01-01",
            2,
            "ihgfed",
            "entry-date",
            2,
            np.nan,
            "rta357",
            np.nan,
            "2023-01-01",
        ],  # No org
    ]
    with open(input_paths[1], "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(",".join(map(str, row)) + "\n")

    # Leave hash3.csv empty except for the headers (to test that an empty csv doesn't screw things up).
    with open(input_paths[2], "w") as f:
        f.write(",".join(columns) + "\n")  # Only write the header row

    # Instantiate the DatasetParquetPackage with temp_dir input paths and a mock schema
    package = DatasetParquetPackage(
        dataset="test_dataset",
        input_paths=input_paths,
        specification_dir=None,
        cache_dir=temp_dir,
    )
    package.create_temp_table(input_paths)

    yield package


def test_load_fact_basic(test_dataset_parquet_package, temp_dir):
    output_dir = temp_dir
    test_dataset_parquet_package.load_facts(
        test_dataset_parquet_package.input_paths, output_dir
    )

    output_file = output_dir / "fact.parquet"
    assert os.path.exists(output_file), "fact.parquet file does not exist"

    df = pd.read_parquet(output_file)
    assert len(df) > 0, "No data in fact.parquet file"
    assert len(df) == 12, "No. of facts is not correct"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"


def test_load_fact_resource_basic(test_dataset_parquet_package, temp_dir):
    output_dir = temp_dir
    test_dataset_parquet_package.load_fact_resource(
        test_dataset_parquet_package.input_paths, output_dir
    )

    # Check if the output parquet file exists and verify contents
    output_file = output_dir / "fact_resource.parquet"
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    # Load Parquet into a DataFrame to verify data correctness
    df = pd.read_parquet(output_file)
    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == 31, "Not all data saved in fact-resource.parquet file"
    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"


def test_load_entities_basic(test_dataset_parquet_package, temp_dir):
    output_dir = temp_dir
    # Create dummy organisation.csv file for use in 'load_entities'
    columns = ["organisation", "entity"]
    # Test data for the tables. This checks that 'field' get pivoted
    data = [
        ["local-authority:AAA", "E06000001"],
        ["local-authority:BBB", "E06000002"],
        ["local-authority:CCC", "E06000003"],
        ["local-authority:DDD", "E06000004"],
        ["local-authority:EEE", "E06000005"],
        ["local-authority:FFF", "E06000006"],
        ["local-authority:GGG", "E06000007"],
        ["local-authority:HHH", "E06000008"],
        ["local-authority:III", "E06000009"],
    ]
    with open(f"{temp_dir}/organisation.csv", "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(",".join(map(str, row)) + "\n")

    test_dataset_parquet_package.load_entities(
        test_dataset_parquet_package.input_paths,
        output_dir,
        f"{temp_dir}/organisation.csv",
    )

    output_file = os.path.join(output_dir, "entity.parquet")
    assert os.path.exists(output_file), "entity.parquet file does not exist"

    df = pd.read_parquet(output_file)
    assert len(df) > 0, "No data in entity.parquet file"
    assert len(df) == 11, "No. of entities is not correct"
    assert df.shape[1] == 16, "Not all columns saved in entity.parquet file"
    assert df["end_date"].isnull().all()  # Check null handling
    assert df["geojson"].isnull().all()  # Check null handling
    assert df["geometry_geom"].isnull().all()  # Check null handling
    assert df["point_geom"].isnull().all()  # Check null handling


def test_load_pq_to_sqlite_basic(test_dataset_parquet_package, temp_dir):
    output_path = os.path.join(temp_dir, "integration_test.sqlite3")
    test_dataset_parquet_package.pq_to_sqlite(output_path, temp_dir)

    assert os.path.exists(output_path), "sqlite3 file does not exist"

    cnx = sqlite3.connect(output_path)
    df_sql = pd.read_sql_query("SELECT * FROM fact_resource", cnx)
    assert len(df_sql) > 0, "No data in fact_resource table"
    assert np.all(
        pd.isnull(df_sql["end-date"])
    ), "Non-empty strings in end_date from fact_resource table"

    df_sql = pd.read_sql_query("SELECT * FROM fact", cnx)
    assert len(df_sql) > 0, "No data in fact table"
    assert np.all(
        pd.isnull(df_sql["end-date"])
    ), "Non-empty strings in end_date from fact table"

    df_sql = pd.read_sql_query("SELECT * FROM entity", cnx)
    assert len(df_sql) > 0, "No data in entity table"
    assert np.any(
        pd.isnull(df_sql["geometry"])
    ), "All geometries from entity table have values"
    cnx.close()
