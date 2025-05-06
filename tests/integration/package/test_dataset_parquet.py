import sqlite3
import numpy as np
import pandas as pd
import logging
import pytest
import os
import json
import pyarrow.parquet as pq
import pyarrow as pa
from digital_land.package.dataset_parquet import DatasetParquetPackage


class MockOrganisation(object):
    def __init__(self, organisation_path):
        self.organisation_path = organisation_path


@pytest.fixture
def org_path(tmp_path):
    org_path = tmp_path / "organisation.csv"
    columns = ["organisation", "entity"]
    # Test data for the tables. This checks that 'field' get pivoted
    data = [
        ["local-authority:AAA", "1"],
        ["local-authority:BBB", "2"],
        ["local-authority:CCC", "3"],
        ["local-authority:DDD", "4"],
        ["local-authority:EEE", "5"],
        ["local-authority:FFF", "6"],
        ["local-authority:GGG", "7"],
        ["local-authority:HHH", "8"],
        ["local-authority:III", "9"],
    ]
    with open(org_path, "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(",".join(map(str, row)) + "\n")
    return org_path


# # Fixture to create a shared temporary directory
# @pytest.fixture(scope="session")
# def temp_dir(tmpdir_factory):
#     temp_dir = tmpdir_factory.mktemp("shared_session_temp_dir")
#     yield temp_dir


@pytest.fixture
def resource_path(tmp_path):
    resource_path = tmp_path / "resource.csv"
    resource_columns = ["resource", "end-date"]
    with open(resource_path, "w") as f:
        f.write(",".join(resource_columns) + "\n")

    return resource_path


# general use file to use for testing should focus  on splitting down into individual test cases
test_geometry = "MULTIPOLYGON(((-0.49901924 53.81622,-0.5177418 53.76114,-0.4268378 53.78454,-0.49901924 53.81622)))"
transformed_1_data = {
    "end_date": [np.nan] * 16,
    "entity": [11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12],
    "entry_date": [
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
    ],
    "entry_number": [2] * 16,
    "fact": [
        "abcdef1",
        "abcdef2",
        "abcdef3",
        "abcdef4",
        "abcdef5",
        "abcdef6",
        "abc1231",
        "abc1232",
        "abc1233",
        "def4561",
        "def4562",
        "def4563",
        "a1b2c31",
        "a1b2c32",
        "a1b2c33",
        "a1b2c34",
    ],
    "field": [
        "entry-date",
        "geometry",
        "point",
        "document-url",
        "organisation",
        "entry-date",
        "geometry",
        "organisation",
        "entry-date",
        "geometry",
        "organisation",
        "entry-date",
        "geomtry",
        "document-url",
        "notes-checking",
        "organisation",
    ],
    "priority": [2] * 16,
    "reference_entity": [np.nan] * 16,
    "resource": [
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "yxwvut",
        "yxwvut",
        "zyxwvu",
        "xwvuts",
        "xwvuts",
        "zyxwvu",
        "wvutsr",
        "wvutsr",
        "wvutsr",
        "wvutsr",
    ],
    "start_date": [np.nan] * 16,
    "value": [
        "2023-01-01",
        f"{test_geometry}",
        '"POINT(-0.481 53.788)"',
        "https://www.test.xyz",
        "organisation:AAA",
        "2023-01-01",
        f"{test_geometry}",
        "local-authority:BBB",
        "2023-01-01",
        f"{test_geometry}",
        "local-authority:CCC",
        "2023-01-01",
        f"{test_geometry}",
        "https://www.testing.yyz",
        "Something random",
        "local-authority:DDD",
    ],
}

transformed_2_data = {
    "end_date": [np.nan] * 19,  # 19 records
    "entity": [
        110,
        110,
        110,
        111,
        111,
        111,
        112,
        112,
        112,
        113,
        113,
        113,
        114,
        114,
        114,
        115,
        115,
        115,
        116,
    ],
    "entry_date": [
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-04-01",
        "2023-05-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
    ],
    "entry_number": [2, 2, 2, 2, 2, 2, 2, 12, 12, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    "fact": [
        "badcfe1",
        "badcfe2",
        "badcfe3",
        "fedcba1",
        "fedcba2",
        "fedcba3",
        "bcdefg1",
        "bcdefg2",
        "bcdefg3",
        "cdefgh1",
        "hgfedc1",
        "cdefgh2",
        "efghij1",
        "efghij2",
        "efghij3",
        "defghi1",
        "defghi2",
        "defghi3",
        "ihgfed1",
    ],
    "field": [
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
        "entry-date",
        "organisation",
        "entry-date",
    ],
    "priority": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 2, 1, 2, 2, 2, 2],
    "reference_entity": [np.nan] * 19,  # 19 records
    "resource": [
        "zyx123",
        "zyx123",
        "zyx123",
        "zxy123",
        "zxy123",
        "zxy123",
        "yxw456",
        "yxw456",
        "yxw456",
        "xwv789",
        "xwv789",
        "xwv789",
        "xyz123",
        "xyz123",
        "xyz123",
        "uvw456",
        "wvu654",
        "uvw456",
        "rta357",
    ],
    "start_date": [np.nan] * 19,  # 19 records
    "value": [
        "2023-01-01",
        "2023-01-01",
        "local-authority:DDD",
        "2023-01-01",
        "2023-02-01",
        "local-authority:EEE",
        "2023-02-01",
        "2023-02-01",
        "local-authority:FFF",
        "2023-01-01",
        "2023-01-01",
        "local-authority:GGG",
        "2023-04-01",
        "2023-05-01",
        "local-authority:HHH",
        "2023-01-01",
        "2023-01-01",
        "local-authority:III",
        "2023-01-01",
    ],
}


@pytest.fixture
def dataset_sqlite_path(tmp_path):
    """
    Should consider using a test spec to feed in to a dataset package instead, also functionality might need to be moved
    """
    sqlite_path = tmp_path / "conservation-area.sqlite3"
    conn = sqlite3.connect(sqlite_path)
    conn.execute(
        """
        CREATE TABLE entity(
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """
    )
    conn.execute(
        """
        CREATE TABLE fact(
            end_date TEXT,
            entity INTEGER,
            fact TEXT PRIMARY KEY,
            field TEXT,
            entry_date TEXT,
            priority INTEGER,
            reference_entity TEXT,
            start_date TEXT,
            value TEXT,
            FOREIGN KEY(entity) REFERENCES entity(entity)
            );
    """
    )
    conn.execute(
        """
        CREATE TABLE fact_resource(
            end_date TEXT,
            fact TEXT,
            entry_date TEXT,
            entry_number INTEGER,
            priority INTEGER,
            resource TEXT,
            start_date TEXT,
            FOREIGN KEY(fact) REFERENCES fact(fact)
        );
    """
    )

    conn.commit()
    conn.close()

    return sqlite_path


@pytest.mark.parametrize("data,expected", [(transformed_1_data, 16)])
def test_load_facts_single_file(data: dict, expected: int, tmp_path):
    """
    tests loading  from a directory  when there is a single file, multiple files
    make very little difference to duckdb so use to test out individual cases
    """
    # convert data to df and save to a file
    df = pd.DataFrame.from_dict(data)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(transformed_parquet_dir / "transformed_resouce.parquet", index=False)

    # instantiate package
    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )

    # this method is explicitly designed to load facts from the temp table
    # however it shouldn't need this, it's duplicating all of the same data in a temporary space
    # we should try leveraging the power of duckdb and parquet.
    package.load_facts(transformed_parquet_dir=transformed_parquet_dir)

    output_file = (
        tmp_path
        / "conservation-area"
        / "fact"
        / "dataset=conservation-area"
        / "fact.parquet"
    )
    assert os.path.exists(output_file), "fact.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact.parquet file"
    assert (
        len(df) == expected
    ), "No. of facts does not match expected"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"


@pytest.mark.parametrize(
    "data1,data2,expected", [(transformed_1_data, transformed_2_data, 35)]
)
def test_load_facts_multiple_files(data1, data2, expected, tmp_path):
    """
    test loading multiple files into the fact table when they're from a single directory
    """
    # convert data to df and save to a file
    df1 = pd.DataFrame.from_dict(data1)
    df2 = pd.DataFrame.from_dict(data2)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df1.to_parquet(
        transformed_parquet_dir / "transformed_resource_1.parquet", index=False
    )
    df2.to_parquet(
        transformed_parquet_dir / "transformed_resource_2.parquet", index=False
    )

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )

    package.load_facts(transformed_parquet_dir=transformed_parquet_dir)

    output_file = (
        tmp_path
        / "conservation-area"
        / "fact"
        / "dataset=conservation-area"
        / "fact.parquet"
    )
    assert os.path.exists(output_file), "fact.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact.parquet file"
    assert (
        len(df) == expected
    ), "No. of facts does not match expected"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"


@pytest.mark.parametrize("data,expected", [(transformed_1_data, 16)])
def test_load_facts_one_file_with_empty_file(data, expected, tmp_path):
    """
    test loading one file into the fact table alongside an empty file
    """

    df = pd.DataFrame.from_dict(data)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(transformed_parquet_dir / "transformed_resouce.parquet", index=False)
    schema = pa.schema(
        [
            ("end_date", pa.string()),
            ("entity", pa.int64()),
            ("entry_date", pa.string()),
            ("entry_number", pa.int64()),
            ("fact", pa.string()),
            ("field", pa.string()),
            ("priority", pa.int64()),
            ("reference_entity", pa.int64()),
            ("resource", pa.string()),
            ("start_date", pa.string()),
            ("value", pa.string()),
        ]
    )
    empty_arrays = [pa.array([], type=field.type) for field in schema]
    empty_table = pa.Table.from_arrays(empty_arrays, schema=schema)
    pq.write_table(empty_table, transformed_parquet_dir / "empty.parquet")

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )

    package.load_facts(transformed_parquet_dir=transformed_parquet_dir)

    output_file = (
        tmp_path
        / "conservation-area"
        / "fact"
        / "dataset=conservation-area"
        / "fact.parquet"
    )
    assert os.path.exists(output_file), "fact.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact.parquet file"
    assert (
        len(df) == expected
    ), "No. of facts does not match expected"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"


@pytest.mark.parametrize(
    "data1,data2,expected", [(transformed_1_data, transformed_2_data, 35)]
)
def test_load_facts_from_temp_parquet(data1, data2, expected, tmp_path):
    """
    tests loading from a directory when there are multiple files, loads them
    into a single parquet file and gets facts from that file
    """
    # convert data to df's and save to a single parquet file
    df1 = pd.DataFrame.from_dict(data1)
    df2 = pd.DataFrame.from_dict(data2)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df1.to_parquet(
        transformed_parquet_dir / "transformed_resource_1.parquet", index=False
    )
    df2.to_parquet(
        transformed_parquet_dir / "transformed_resource_2.parquet", index=False
    )

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    temp_parquet = package.load_details_into_temp_parquet(
        transformed_parquet_dir=transformed_parquet_dir
    )
    assert os.path.exists(temp_parquet), "temp parquet file not created"
    package.load_facts_from_temp_parquet(
        transformed_parquet_dir=transformed_parquet_dir, temp_parquet=temp_parquet
    )

    output_file = (
        tmp_path
        / "conservation-area"
        / "fact"
        / "dataset=conservation-area"
        / "fact.parquet"
    )
    assert os.path.exists(output_file), "fact.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact.parquet file"
    assert (
        len(df) == expected
    ), "No. of facts does not match expected"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"


@pytest.mark.parametrize(
    "data_1,data_2,expected", [(transformed_1_data, transformed_2_data, 35)]
)
def test_load_fact_resource_from_temp_parquet(data_1, data_2, expected, tmp_path):
    """
    tests loading from a directory when there are multiple files, loads them
    into a single parquet file and gets fact_resources from that file
    """
    # convert data to df's and save to a single parquet file
    df_1 = pd.DataFrame.from_dict(data_1)
    df_2 = pd.DataFrame.from_dict(data_2)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df_1.to_parquet(
        transformed_parquet_dir / "transformed_resource_1.parquet", index=False
    )
    df_2.to_parquet(
        transformed_parquet_dir / "transformed_resource_2.parquet", index=False
    )

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    temp_parquet = package.load_details_into_temp_parquet(
        transformed_parquet_dir=transformed_parquet_dir
    )
    assert os.path.exists(temp_parquet), "temp parquet file not created"
    package.load_fact_resource_from_temp_parquet(
        transformed_parquet_dir=transformed_parquet_dir, temp_parquet=temp_parquet
    )

    # Check if the output parquet file exists and verify contents
    output_file = (
        tmp_path
        / "conservation-area"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    # Load Parquet into a DataFrame to verify data correctness
    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == expected, "Not all data saved in fact-resource.parquet file"

    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"


@pytest.mark.parametrize("data,expected", [(transformed_1_data, 16)])
def test_load_fact_resource_single_file(data, expected, tmp_path):

    df = pd.DataFrame.from_dict(data)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(transformed_parquet_dir / "transformed_resouce.parquet", index=False)

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    package.load_fact_resource(transformed_parquet_dir)

    # Check if the output parquet file exists and verify contents
    output_file = (
        tmp_path
        / "conservation-area"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    # Load Parquet into a DataFrame to verify data correctness
    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == expected, "Not all data saved in fact-resource.parquet file"

    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"


@pytest.mark.parametrize(
    "data_1,data_2,expected", [(transformed_1_data, transformed_2_data, 35)]
)
def test_load_fact_resource_two_filea(data_1, data_2, expected, tmp_path):
    df_1 = pd.DataFrame.from_dict(data_1)
    df_2 = pd.DataFrame.from_dict(data_2)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df_1.to_parquet(
        transformed_parquet_dir / "transformed_resource_1.parquet", index=False
    )
    df_2.to_parquet(
        transformed_parquet_dir / "transformed_resource_2.parquet", index=False
    )

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    package.load_fact_resource(transformed_parquet_dir)

    # Check if the output parquet file exists and verify contents
    output_file = (
        tmp_path
        / "conservation-area"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    # Load Parquet into a DataFrame to verify data correctness
    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == expected, "Not all data saved in fact-resource.parquet file"

    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"


@pytest.mark.parametrize("data,expected", [(transformed_1_data, 16)])
def test_load_fact_resource_empty_file_with_another(data, expected, tmp_path):

    df = pd.DataFrame.from_dict(data)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(transformed_parquet_dir / "transformed_resouce.parquet", index=False)
    # create empty file
    schema = pa.schema(
        [
            ("end_date", pa.string()),
            ("entity", pa.int64()),
            ("entry_date", pa.string()),
            ("entry_number", pa.int64()),
            ("fact", pa.string()),
            ("field", pa.string()),
            ("priority", pa.int64()),
            ("reference_entity", pa.int64()),
            ("resource", pa.string()),
            ("start_date", pa.string()),
            ("value", pa.string()),
        ]
    )
    empty_arrays = [pa.array([], type=field.type) for field in schema]
    empty_table = pa.Table.from_arrays(empty_arrays, schema=schema)
    pq.write_table(empty_table, transformed_parquet_dir / "empty.parquet")

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    package.load_fact_resource(transformed_parquet_dir)

    # Check if the output parquet file exists and verify contents
    output_file = (
        tmp_path
        / "conservation-area"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    # Load Parquet into a DataFrame to verify data correctness
    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == expected, "Not all data saved in fact-resource.parquet file"

    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"


@pytest.mark.parametrize(
    "data,expected_count,expected_props",
    # need to buid an example where organisation is blank
    [
        (transformed_1_data, 2, {11: {"end_date": ""}}),
        (
            {
                "end_date": [np.nan],  # 19 records
                "entity": [
                    110,
                ],
                "entry_date": [
                    "2023-01-01",
                ],
                "entry_number": [2],
                "fact": [
                    "badcfe1",
                ],
                "field": [
                    "entry-date",
                ],
                "priority": [2],
                "reference_entity": [np.nan],  # 19 records
                "resource": [
                    "zyx123",
                ],
                "start_date": [np.nan],  # 19 records
                "value": ["2023-01-01"],
            },
            1,
            {},
        ),
    ],
)
def test_load_entities_single_file(
    data, expected_count, expected_props, tmp_path, org_path, resource_path
):
    # Create dummy organisation.csv file for use in 'load_entities'
    # Test data for the tables. This checks that 'field' get pivoted
    df = pd.DataFrame.from_dict(data)
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(transformed_parquet_dir / "transformed_resouce.parquet", index=False)

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
    )
    package.load_entities(transformed_parquet_dir, resource_path, org_path)

    output_file = (
        tmp_path
        / "conservation-area"
        / "entity"
        / "dataset=conservation-area"
        / "entity.parquet"
    )
    assert os.path.exists(output_file), "entity.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in entity.parquet file"
    assert len(df) == expected_count, "No. of entities is not correct"
    assert df["entity"].nunique() == len(df), "Entity column contains duplicate values"

    for entity in expected_props:
        for key, value in expected_props[entity].items():
            logging.info(f"entity={entity}, key={key}, value={value}")
            assert (
                df[df["entity"] == entity][key].iloc[0] == value
            ), f"Expected {key} to be {value} for entity {entity}"


# not  great test as have to feed so much in, would be  better to test each table  loading at a time
@pytest.mark.parametrize(
    "fact_data,fact_resource_data,entity_data",
    [
        (
            {
                "fact": [""],
                "end_date": [1],
                "entity": [1],
                "field": [""],
                "entry_date": [""],
                "priority": [1],
                "reference_entity": [""],
                "start_date": [1],
                "value": [""],
            },
            {
                "end_date": [""],
                "fact": [1],
                "entry_date": [""],
                "entry_number": [1],
                "priority": [1],
                "resource": [""],
                "start_date": [1],
            },
            {
                "entity": [1],
                "dataset": ["conservation-area"],
                "end_date": [""],
                "entry_date": [""],
                "geojson": [""],
                "geometry": [""],
                "json": [""],
                "name": [""],
                "organisation_entity": [""],
                "point": [""],
                "prefix": [""],
                "reference": [""],
                "start_date": [""],
                "typology": [""],
            },
        )
    ],
)
def test_load_pq_to_sqlite_basic(
    fact_data, fact_resource_data, entity_data, dataset_sqlite_path, tmp_path
):

    dataset_parquet_path = tmp_path / "dataset"
    (dataset_parquet_path / "dataset=conservation-area").mkdir(
        parents=True, exist_ok=True
    )
    # write data to parquet files in the dataset path
    fact_df = pd.DataFrame.from_dict(fact_data)
    fact_resource_df = pd.DataFrame.from_dict(fact_resource_data)
    entity_df = pd.DataFrame.from_dict(entity_data)

    (dataset_parquet_path / "fact" / "dataset=conservation-area").mkdir(
        parents=True, exist_ok=True
    )
    (dataset_parquet_path / "fact-resource" / "dataset=conservation-area").mkdir(
        parents=True, exist_ok=True
    )
    (dataset_parquet_path / "entity" / "dataset=conservation-area").mkdir(
        parents=True, exist_ok=True
    )

    fact_df.to_parquet(
        dataset_parquet_path / "fact" / "dataset=conservation-area" / "fact.parquet",
        index=False,
    )
    fact_resource_df.to_parquet(
        dataset_parquet_path
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet",
        index=False,
    )
    entity_df.to_parquet(
        dataset_parquet_path
        / "entity"
        / "dataset=conservation-area"
        / "entity.parquet",
        index=False,
    )

    output_path = dataset_sqlite_path

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "dataset",
        specification_dir=None,
    )

    package.load_to_sqlite(output_path)

    assert os.path.exists(dataset_sqlite_path), "sqlite3 file does not exist"

    cnx = sqlite3.connect(output_path)
    df_sql = pd.read_sql_query("SELECT * FROM fact_resource", cnx)
    assert len(df_sql) > 0, "No data in fact_resource table"
    assert len(df_sql) == len(
        fact_resource_df
    ), "Not all data saved in fact_resource table"
    assert np.all(
        len(df_sql["end_date"] == 0)
    ), "Non-empty strings in end_date from fact_resource table"

    df_sql = pd.read_sql_query("SELECT * FROM fact", cnx)
    assert len(df_sql) > 0, "No data in fact table"
    assert len(df_sql) == len(fact_df), "Not all data saved in fact table"
    assert np.all(
        len(df_sql["end_date"] == 0)
    ), "Non-empty strings in end_date from fact table"

    df_sql = pd.read_sql_query("SELECT * FROM entity", cnx)
    assert len(df_sql) > 0, "No data in entity table"
    assert len(df_sql) == len(entity_df), "Not all data saved in entity table"
    assert np.any(
        len(df_sql["geometry"] == 0)
    ), "All geometries from entity table have values"
    assert np.any(
        len(df_sql["geometry"] == 0)
    ), "All geometries from entity table have non-blank values"
    assert not any(
        [
            (
                any("_" in key for key in json.loads(row).keys())
                if isinstance(row, str)
                else False
            )
            for row in df_sql["json"]
            if row != ""
        ]
    ), "Some json object have underscores in their 'keys'"

    cnx.close()
