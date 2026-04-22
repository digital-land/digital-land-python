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
import random
import string


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
            typology TEXT,
            quality TEXT
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
    "data1,data2,expected_facts,expected_entities",
    [(transformed_1_data, transformed_2_data, 35 * 100 / 2, 3 * 100)],
)
def test_load_functions_batch(
    data1, data2, expected_facts, expected_entities, tmp_path, org_path, resource_path
):
    """
    test loading multiple files into the fact table when they're from a single directory and batched into a single file
    """
    # Use current data, but edit the fact and entities so that we have many more facts and entities than current test
    # data gives us
    df1 = pd.DataFrame.from_dict(data1)
    df2 = pd.DataFrame.from_dict(data2)
    df1_copy = df1.copy()
    df2_copy = df2.copy()
    ldf1 = len(df1_copy)
    one_third1 = ldf1 // 3
    two_third1 = 2 * ldf1 // 3
    ldf2 = len(df2_copy)
    one_third2 = ldf2 // 3
    two_third2 = 2 * ldf2 // 3
    transformed_parquet_dir = tmp_path / "transformed"
    transformed_parquet_dir.mkdir(parents=True, exist_ok=True)
    random.seed(42)  # Set seed to ensure consistency
    chars = string.ascii_lowercase + string.digits
    # Create 100 files so that strategy is 'batch'
    for i in range(100):
        # Change the fact and entity values to ensure uniqueness
        base = i * 10  # base entity value (assigned to first third of rows)
        if i % 2 == 0:
            df1_copy = df1_copy.assign(
                fact=["".join(random.choices(chars, k=10)) for _ in range(ldf1)],
                entity=[base] * one_third1
                + [base + 1] * (two_third1 - one_third1)
                + [base + 2] * (ldf1 - two_third1),
            )
            df1_copy.to_parquet(
                transformed_parquet_dir / f"transformed_resource_{i}.parquet",
                index=False,
            )
        else:
            df2_copy = df2_copy.assign(
                fact=["".join(random.choices(chars, k=10)) for _ in range(ldf2)],
                entity=[base] * one_third2
                + [base + 1] * (two_third2 - one_third2)
                + [base + 2] * (ldf2 - two_third2),
            )
            df2_copy.to_parquet(
                transformed_parquet_dir / f"transformed_resource_{i}.parquet",
                index=False,
            )

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "conservation-area",
        specification_dir=None,
        transformed_parquet_dir=transformed_parquet_dir,
    )

    package.group_parquet_files(transformed_parquet_dir=transformed_parquet_dir)
    package.load_facts(transformed_parquet_dir=transformed_parquet_dir)
    package.load_fact_resource(transformed_parquet_dir=transformed_parquet_dir)
    package.load_entities(transformed_parquet_dir, resource_path, org_path)

    # test facts
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
        len(df) == expected_facts
    ), "No. of facts does not match expected"  # No of unique facts
    assert df.shape[1] == 9, "Not all columns saved in fact.parquet file"

    # test fact_resource
    output_file = (
        tmp_path
        / "conservation-area"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    assert os.path.exists(output_file), "fact-resource.parquet file does not exist"

    df = pd.read_parquet(output_file)

    assert len(df) > 0, "No data in fact-resource,parquet file"
    assert len(df) == expected_facts, "Not all data saved in fact-resource.parquet file"

    assert df.shape[1] == 7, "Not all columns saved in fact-resource.parquet file"

    # test entities
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
    assert len(df) == expected_entities, "No. of entities is not correct"
    assert df["entity"].nunique() == len(df), "Entity column contains duplicate values"


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
        (
            transformed_1_data,
            2,
            {
                11: {"end_date": ""},
            },
        ),
        (
            transformed_2_data,
            7,
            {
                114: {"quality": "authoritative"},
            },
        ),
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
                "reference_entity": [np.nan],  # 19 records
                "resource": [
                    "zyx123",
                ],
                "start_date": [np.nan],  # 19 records
                "value": ["2023-01-01"],
                "priority": [1],
            },
            1,
            {110: {"quality": "some"}},
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
    entities = list(df["entity"])
    assert (
        len(df) == expected_count
    ), f"No. of entities is not correct, entities are: {entities}"

    assert df["entity"].nunique() == len(df), "Entity column contains duplicate values"
    for entity in expected_props:
        for key, value in expected_props[entity].items():
            logging.info(f"entity={entity}, key={key}, value={value}")
            actual_value = df[df["entity"] == entity][key].iloc[0]
            assert (
                actual_value == value
            ), f"Expected {key} to be {value} for entity {entity} but value is {actual_value}"
    # test non expected columns in entity table which may accidenltally get added
    not_expected_fields = ["priority"]
    for field in not_expected_fields:
        assert (
            field not in df.columns
        ), f"Expected field {field} not found in entity table"


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
                "quality": ["authoritative"],
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


def test_multi_bucket_load_facts_no_fact_loss(tmp_path):
    """
    Exercises the multi-bucket path in load_facts end-to-end and asserts that
    every unique fact in the input appears exactly once in fact.parquet.

    The multi-bucket path is forced by:
    - creating one small parquet file per fact so group_parquet_files produces
      many batch files (one per source file with target_mb=0.001)
    - overriding parquet_dir_details to simulate a large dataset relative to
      available memory, pushing n_buckets above 1

    If any facts are silently dropped during bucket assignment or the final merge
    this test will fail.
    """
    n_facts = 50
    unique_facts = [f"{'a' * 63}{i}" for i in range(n_facts)]

    transformed_dir = tmp_path / "transformed"
    transformed_dir.mkdir()

    for i, fact_hash in enumerate(unique_facts):
        pd.DataFrame(
            {
                "end_date": [""],
                "entity": [i + 1],
                "entry_date": ["2023-01-01"],
                "entry_number": ["1"],
                "fact": [fact_hash],
                "field": ["name"],
                "priority": ["1"],
                "reference_entity": [""],
                "resource": ["resource_abc"],
                "start_date": [""],
                "value": [f"value_{i}"],
            }
        ).to_parquet(transformed_dir / f"resource_{i}.parquet", index=False)

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "output",
        specification_dir=None,
        transformed_parquet_dir=transformed_dir,
    )

    # One source file per batch, giving n_facts batch files
    package.group_parquet_files(transformed_dir, target_mb=0.001)

    # Simulate large-dataset conditions so n_buckets > 1 is calculated
    package.parquet_dir_details["total_size_mb"] = 100.0
    package.parquet_dir_details["memory_available"] = 1.0

    package.load_facts(transformed_dir)

    output_file = (
        tmp_path / "output" / "fact" / "dataset=conservation-area" / "fact.parquet"
    )
    assert output_file.exists(), "fact.parquet was not created"

    df_result = pd.read_parquet(output_file)

    assert len(df_result) == n_facts, (
        f"Expected {n_facts} facts but got {len(df_result)}. "
        "The multi-bucket path silently dropped some facts."
    )


def test_assign_to_buckets_creates_correct_files(tmp_path):
    """
    Tests that _assign_to_buckets creates exactly n_buckets files in bucket_dir
    and that the total row count across all bucket files equals the total row
    count in the batch files (no rows dropped or duplicated during assignment).
    """
    n_facts = 30
    n_buckets = 3

    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    for i in range(n_facts):
        pd.DataFrame(
            {
                "end_date": [""],
                "entity": [i + 1],
                "entry_date": ["2023-01-01"],
                "entry_number": ["1"],
                "fact": [f"{'a' * 63}{i}"],
                "field": ["name"],
                "priority": ["1"],
                "reference_entity": [""],
                "resource": ["resource_abc"],
                "start_date": [""],
                "value": [f"value_{i}"],
            }
        ).to_parquet(batch_dir / f"batch_{i:02}.parquet", index=False)

    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir()

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "output",
        specification_dir=None,
    )
    bucket_paths = package._assign_to_buckets(batch_dir, bucket_dir, n_buckets)

    assert (
        len(bucket_paths) == n_buckets
    ), f"Expected {n_buckets} bucket files, got {len(bucket_paths)}"
    assert all(p.exists() for p in bucket_paths), "Not all bucket files were created"

    bucket_total = sum(len(pd.read_parquet(p)) for p in bucket_paths)
    assert bucket_total == n_facts, (
        f"Expected {n_facts} total rows across buckets, got {bucket_total}. "
        "Rows were dropped or duplicated during bucket assignment."
    )

    # each fact hash should appear in exactly one bucket
    fact_counts = {}
    for p in bucket_paths:
        for fact in pd.read_parquet(p)["fact"]:
            fact_counts[fact] = fact_counts.get(fact, 0) + 1
    assert all(
        v == 1 for v in fact_counts.values()
    ), "Some fact hashes appear in more than one bucket"


def test_deduplicate_buckets_creates_correct_files(tmp_path):
    """
    Tests that _deduplicate_buckets creates exactly one result file per bucket
    and that duplicate fact hashes within a bucket are reduced to a single row,
    keeping the row with the latest entry_date.
    """
    n_buckets = 2

    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir()

    # bucket_00: two rows for the same fact — only the later entry_date should survive
    pd.DataFrame(
        {
            "end_date": ["", ""],
            "entity": [1, 1],
            "entry_date": ["2023-01-01", "2023-06-01"],
            "entry_number": ["1", "2"],
            "fact": ["fact_aaa", "fact_aaa"],
            "field": ["name", "name"],
            "priority": ["1", "1"],
            "reference_entity": ["", ""],
            "resource": ["res_a", "res_b"],
            "start_date": ["", ""],
            "value": ["old_value", "new_value"],
        }
    ).to_parquet(bucket_dir / "bucket_00.parquet", index=False)

    # bucket_01: two distinct facts, no duplicates
    pd.DataFrame(
        {
            "end_date": ["", ""],
            "entity": [2, 3],
            "entry_date": ["2023-01-01", "2023-01-01"],
            "entry_number": ["1", "1"],
            "fact": ["fact_bbb", "fact_ccc"],
            "field": ["name", "name"],
            "priority": ["1", "1"],
            "reference_entity": ["", ""],
            "resource": ["res_a", "res_a"],
            "start_date": ["", ""],
            "value": ["value_b", "value_c"],
        }
    ).to_parquet(bucket_dir / "bucket_01.parquet", index=False)

    result_dir = tmp_path / "result"
    result_dir.mkdir()

    package = DatasetParquetPackage(
        dataset="conservation-area",
        path=tmp_path / "output",
        specification_dir=None,
    )
    bucket_paths = [bucket_dir / "bucket_00.parquet", bucket_dir / "bucket_01.parquet"]
    result_paths = package._deduplicate_buckets(bucket_paths, result_dir)

    assert (
        len(result_paths) == n_buckets
    ), f"Expected {n_buckets} result files, got {len(result_paths)}"
    assert all(p.exists() for p in result_paths), "Not all result files were created"

    df_00 = pd.read_parquet(result_paths[0])
    assert (
        len(df_00) == 1
    ), "Duplicate fact in bucket_00 should be deduplicated to 1 row"
    assert (
        df_00.iloc[0]["value"] == "new_value"
    ), "The row with the latest entry_date should be kept"

    df_01 = pd.read_parquet(result_paths[1])
    assert len(df_01) == 2, "bucket_01 has 2 distinct facts, both should be kept"
