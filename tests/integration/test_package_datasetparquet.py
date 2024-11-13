import tempfile

import numpy as np
import pandas as pd
import pytest
import os
from digital_land.package.datasetparquet import DatasetParquetPackage


@pytest.fixture
def test_dataset_parquet_package():
    # Set up temporary files with mock data
    with tempfile.TemporaryDirectory() as temp_dir:
        input_paths = [
            os.path.join(temp_dir, 'hash1.csv'),
            os.path.join(temp_dir, 'hash2.csv'),
            os.path.join(temp_dir, 'hash3.csv')
        ]
        columns = [
            'end-date', 'entity', 'entry-date', 'entry-number', 'fact', 'field', 'priority', 'reference-entity',
            'resource', 'start-date', 'value'
        ]
        # Test data for the tables. This checks that 'field' get pivoted
        test_geometry = \
            "MULTIPOLYGON(((-0.49901924 53.81622,-0.5177418 53.76114,-0.4268378 53.78454,-0.49901924 53.81622)))"
        data = [
            [np.nan, 11, '2023-01-01', 2, 'abcdef', 'entry-date', 2, np.nan, 'zyxwvu', np.nan, '2023-01-01'],
            [np.nan, 11, '2023-02-01', 2, 'abcdef', 'geometry', 2, np.nan, 'zyxwvu', np.nan, f"{test_geometry}"],
            [np.nan, 12, '2023-02-01', 2, 'abc123', 'entry-date', 2, np.nan, 'yxwvut', np.nan, '2023-01-01'],
            [np.nan, 12, '2023-02-01', 2, 'abc123', 'geometry', 2, np.nan, 'yxwvut', np.nan, f"{test_geometry}"],
            [np.nan, 13, '2023-01-01', 2, 'def456', 'entry-date', 2, np.nan, 'xwvuts', np.nan, '2023-01-01'],
            [np.nan, 13, '2023-01-01', 2, 'def456', 'geometry', 2, np.nan, 'xwvuts', np.nan, f"{test_geometry}"],
            [np.nan, 14, '2023-01-01', 2, 'a1b2c3', 'entry-date', 2, np.nan, 'wvutsr', np.nan, '2023-01-01'],
            [np.nan, 14, '2023-01-01', 2, 'a1b2c3', 'geometry', 2, np.nan, 'wvutsr', np.nan, f"{test_geometry}"]
        ]
        with open(input_paths[0], 'w') as f:
            # Write header
            f.write(','.join(columns) + '\n')
            # Write sample data
            for row in data:
                f.write(','.join(map(str, row)) + '\n')

        # Test data for the tables. This has plenty of 'duplicates' to check
        data = [
            [np.nan, 111, '2023-01-01', 2, 'fedcba', 'entry-date', 2, np.nan, 'zyx123', np.nan, '2023-01-01'],
            [np.nan, 111, '2023-02-01', 2, 'fedcba', 'entry-date', 2, np.nan, 'zyx123', np.nan, '2023-02-01'], #ent-date
            [np.nan, 112, '2023-02-01', 2, 'bcdefg', 'entry-date', 2, np.nan, 'yxw456', np.nan, '2023-02-01'],
            [np.nan, 112, '2023-02-01', 12, 'bcdefg', 'entry-date', 2, np.nan, 'yxw456', np.nan, '2023-02-01'], #ent-no
            [np.nan, 113, '2023-01-01', 2, 'cdefgh', 'entry-date', 2, np.nan, 'xwv789', np.nan, '2023-01-01'],
            [np.nan, 113, '2023-01-01', 2, 'hgfedc', 'entry-date', 2, np.nan, 'xwv789', np.nan, '2023-01-01'],  # fact
            [np.nan, 114, '2023-04-01', 2, 'efghij', 'entry-date', 1, np.nan, 'xyz123', np.nan, '2023-04-01'],
            [np.nan, 114, '2023-04-01', 2, 'efghij', 'entry-date', 2, np.nan, 'xyz123', np.nan, '2023-04-01'],# priority
            [np.nan, 115, '2023-01-01', 2, 'defghi', 'entry-date', 2, np.nan, 'uvw456', np.nan, '2023-01-01'],
            [np.nan, 115, '2023-01-01', 2, 'defghi', 'entry-date', 2, np.nan, 'wvu654', np.nan, '2023-01-01']  #resource
        ]
        with open(input_paths[1], 'w') as f:
            # Write header
            f.write(','.join(columns) + '\n')
            # Write sample data
            for row in data:
                f.write(','.join(map(str, row)) + '\n')

        # Leave hash3.csv empty except for the headers
        with open(input_paths[2], 'w') as f:
            f.write(','.join(columns) + '\n')  # Only write the header row

        # Instantiate the DatasetParquetPackage with temp_dir input paths and a mock schema
        package = DatasetParquetPackage(
            dataset="test_dataset",
            input_paths=input_paths,
            specification_dir=None
        )
        print(temp_dir)

        yield package


# @pytest.fixture
# def dataset_parquet_package():
#     # Instantiate the DatasetParquetPackage with dummy data
#     return DatasetParquetPackage(dataset="test_dataset", input_paths=[])
#
#
# def test_load_fact_resource_basic(mock_input_paths, dataset_parquet_package):
#     # Run load_fact_resource
#     output_dir = tempfile.mkdtemp()
#     test_dataset_parquet_package.load_fact_resource(test_dataset_parquet_package, output_dir)
#
#     # Check if the output parquet file exists and verify contents
#     output_file = os.path.join(output_dir, 'fact_resource.parquet')
#     assert os.path.exists(output_file)
#
#     # Load Parquet into a DataFrame to verify data correctness
#     df = pd.read_parquet(output_file)
#     assert len(df) > 0  # Verify data was written correctly
#     assert 'fact' in df.columns
#     assert 'resource' in df.columns
#
#
# def test_load_facts_duplicate_handling(mock_input_paths, dataset_parquet_package):
#     # Write data to mock CSV input with duplicate entries to test deduplication
#     with open(test_dataset_parquet_package[0], 'w') as f:
#         f.write('fact,priority,entry-date,entry-number\n')
#         f.write('fact1,1,2023-01-01,10\n')  # Higher priority
#         f.write('fact1,2,2023-01-02,5\n')  # Lower priority, should be ignored
#
#     output_dir = tempfile.mkdtemp()
#     test_dataset_parquet_package.load_facts(test_dataset_parquet_package, output_dir)
#
#     output_file = os.path.join(output_dir, 'fact.parquet')
#     df = pd.read_parquet(output_file)
#     assert len(df) == 1  # Should only have one entry due to deduplication
#     assert df['priority'].iloc[0] == 1  # Check the highest-priority entry
#
#
# def test_load_entities_null_handling(mock_input_paths, dataset_parquet_package):
#     with open(test_dataset_parquet_package[0], 'w') as f:
#         f.write('entity,field,value\n')
#         f.write('entity1,field1,value1\n')
#         f.write('entity1,field2,\n')  # Null value
#
#     output_dir = tempfile.mkdtemp()
#     test_dataset_parquet_package.load_entities(test_dataset_parquet_package, output_dir)
#
#     output_file = os.path.join(output_dir, 'entity.parquet')
#     df = pd.read_parquet(output_file)
#     assert 'field2' in df.columns
#     assert df['field2'].isnull().any()  # Check null handling


# # import json
# # import os.path
# import pytest
#
# from csv import DictReader
#
# from digital_land.commands import dataset_create
# # from digital_land.specification import specification_path
# # from digital_land.specification import Specification
#
# @pytest.mark.parametrize(
#     "dataset_name",
#     [
#         "entity",
#         "fact",
#         "fact-resource",
#     ],
# )
# def test_package_datasetparquet(
#     # Parametrize args
#     dataset_name,
#     # # Static runtime filesystem dependencies
#     # column_field_dir,
#     # dataset_resource_dir,
#     organisation_path,
#     # # Runtime filesystem dependencies generated by previous steps
#     # transformed_dir,
#     # pipeline_dir,
#     # Test assertion directories
#     dataset_dir,
#     # Pytest fixtures
#     tmp_path,
# ):
#     # Setup
#     expected_pq_result = dataset_dir.joinpath(f"{dataset_name}.parquet")
#
#     input_paths = [
#         str(transformed_path)
#         for transformed_path in transformed_dir.joinpath(dataset_name).iterdir()
#     ]
#
#     output_dir = tmp_path.joinpath("dataset_output")
#     output_dir.mkdir()
#     csv_path = output_dir.joinpath(f"{dataset_name}.csv")
#     parquet_path = output_dir.joinpath(f"{dataset_name}.parquet")
#     # sqlite_path = output_dir.join path(f"{dataset_name}.sqlite3")
#
#     dataset_create(input_paths, parquet_path, organisation_path)
#     # dataset_dump(sqlite_path, parquet_path)
#
#     # Assert
#     with parquet_path.open() as actual, expected_pq_result.open() as expected:
#         actual_dict_reader = DictReader(actual)
#         expected_dict_reader = DictReader(expected)
#         assert actual_dict_reader.fieldnames == expected_dict_reader.fieldnames
#         assert list(actual_dict_reader) == list(expected_dict_reader)
