import pytest
import os
import csv
import pandas as pd
from digital_land.pipeline import Pipeline
from digital_land.pipeline import Lookups


def write_as_csv(dir, filename, data):
    with open(os.path.join(dir, filename), "w") as f:
        if data.__class__ is list:
            dictwriter = csv.DictWriter(f, fieldnames=data[0].keys())
            dictwriter.writeheader()
            dictwriter.writerows(data)
        else:
            dictwriter = csv.DictWriter(f, fieldnames=data.keys())
            dictwriter.writeheader()
            dictwriter.writerow(data)


@pytest.fixture
def pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    # create lookups
    write_as_csv(
        pipeline_dir,
        "lookup.csv",
        {
            "prefix": "ancient-woodland",
            "resource": "",
            "organisation": "local-authority-eng:ABC",
            "reference": "ABC_0001",
            "entity": "1234567",
        },
    )

    return pipeline_dir


@pytest.fixture
def empty_pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    return pipeline_dir


def get_test_column_csv_data_with_resources_and_endpoints(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    return {
        "dataset": [pipeline, pipeline, pipeline],
        "resource": [
            "",
            resource,
            "",
        ],
        "endpoint": [
            "",
            "",
            endpoint,
        ],
        "column": [
            "NAME",
            "OBJECTID",
            "dummy_fiel",
        ],
        "field": [
            "name",
            "res-field-one",
            "ep-field-one",
        ],
    }


def get_test_column_csv_data_with_resource_endpoint_clash_ep_first(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    return {
        "dataset": [
            pipeline,
            pipeline,
            pipeline,
        ],
        "resource": ["", "", resource],
        "endpoint": ["", endpoint, ""],
        "column": [
            "NAME",
            "REF",
            "REF",
        ],
        "field": ["name", "ep-ref", "res-ref"],
    }


def get_test_column_csv_data_with_resource_endpoint_clash_res_first(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    return {
        "dataset": [
            pipeline,
            pipeline,
            pipeline,
        ],
        "resource": ["", resource, ""],
        "endpoint": ["", "", endpoint],
        "column": [
            "NAME",
            "REF",
            "REF",
        ],
        "field": ["name", "res-ref", "ep-ref"],
    }


def get_test_concat_csv_data_with_endpoints_and_resources(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    return {
        "dataset": [pipeline, pipeline, pipeline],
        "resource": [resource, "", resource],
        "endpoint": ["", endpoint, endpoint],
        "field": ["o-field1", "o-field2", "o-field3"],
        "fields": ["i-field1;i-field2", "i-field3;i-field4", "i-field5;i-field6"],
        "separator": [".", ".", "."],
        "entry-date": ["", "", ""],
        "start-date": ["", "", ""],
        "end-date": ["", "", ""],
    }


def test_load_column_when_csv_contains_endpoints_and_resources(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    # create the datasource file used by the Pipeline class
    raw_data = get_test_column_csv_data_with_resources_and_endpoints(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    column_keys = list(pipeline.column.keys())
    column_values = list(pipeline.column.values())

    # -- Assert --
    assert test_resource in column_keys
    assert test_endpoint in column_keys

    assert {"name": "name"} in column_values
    assert {"objectid": "res-field-one"} in column_values
    assert {"dummy-fiel": "ep-field-one"} in column_values


def test_load_concat_when_csv_contains_endpoints_and_resources(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    # create the datasource file used by the Pipeline class
    raw_data = get_test_concat_csv_data_with_endpoints_and_resources(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/concat.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    concat_keys = list(pipeline.concat.keys())
    concat_values = list(pipeline.concat.values())

    # -- Assert --
    # resource
    assert test_resource in concat_keys

    assert "o-field1" in concat_values[0].keys()

    assert "i-field1" in concat_values[0]["o-field1"]["fields"]
    assert "i-field2" in concat_values[0]["o-field1"]["fields"]

    assert "i-field5" in concat_values[0]["o-field3"]["fields"]
    assert "i-field6" in concat_values[0]["o-field3"]["fields"]

    # endpoint
    assert test_endpoint in concat_keys

    assert "o-field2" in concat_values[1].keys()

    assert "i-field3" in concat_values[1]["o-field2"]["fields"]
    assert "i-field4" in concat_values[1]["o-field2"]["fields"]

    # when a resource and endpoint are present in a concat data row, resource takes priority
    assert "o-field3" in concat_values[0].keys()
    assert "o-field3" not in concat_values[1].keys()


def test_columns():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    column = p.columns()

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
        "quatre": "four",
    }


def test_resource_column_mapping_takes_priority():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    column = p.columns("some-resource")

    assert "quatre" not in column
    assert "quatro" in column
    assert "un" in column
    assert "due" in column


def test_resource_specific_columns():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    column = p.columns("some-resource")

    assert (
        list(column)[0] == "quatro"
    ), "resource specific column 'quatro' should appear first in the returned dict"

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
        "quatro": "four",
    }


def test_columns_when_csv_contains_endpoints_and_resources(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    raw_data = get_test_column_csv_data_with_resources_and_endpoints(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    columns = pipeline.columns(test_resource, test_endpoints)

    # -- Assert --
    assert columns["name"] == "name"
    assert columns["objectid"] == "res-field-one"
    assert columns["dummy-fiel"] == "ep-field-one"


def test_columns_when_csv_contains_clashing_entries_res_first(tmp_path):
    """
    Check that resource column mapping definitions take priority over
    endpoint column mapping definitions.
    This test ensures correct functionality when the resource column mapping
    definition appears first in the column.csv file
    :param tmp_path:
    :return: None
    """
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    raw_data = get_test_column_csv_data_with_resource_endpoint_clash_res_first(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    columns = pipeline.columns(test_resource, test_endpoints)

    # -- Assert --
    assert columns["name"] == "name"
    assert columns["ref"] == "res-ref"
    assert "ep_ref" not in columns


def test_columns_when_csv_contains_clashing_entries_ep_first(tmp_path):
    """
    Check that resource column mapping definitions take priority over
    endpoint column mapping definitions.
    This test ensures correct functionality when the endpoint column mapping
    definition appears first in the column.csv file
    :param tmp_path:
    :return: None
    """
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    raw_data = get_test_column_csv_data_with_resource_endpoint_clash_ep_first(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    columns = pipeline.columns(test_resource, test_endpoints)

    # -- Assert --
    assert columns["name"] == "name"
    assert columns["ref"] == "res-ref"
    assert "ep_ref" not in columns


def test_concatenations_when_csv_contains_endpoints_and_resources(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    # create the datasource file used by the Pipeline class
    raw_data = get_test_concat_csv_data_with_endpoints_and_resources(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/concat.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    concats = pipeline.concatenations(test_resource, test_endpoints)

    # -- Assert --
    # resource
    concats_keys = concats.keys()

    assert "o-field1" in concats_keys
    assert "o-field3" in concats_keys

    assert "i-field1" in concats["o-field1"]["fields"]
    assert "i-field2" in concats["o-field1"]["fields"]

    assert "i-field5" in concats["o-field3"]["fields"]
    assert "i-field6" in concats["o-field3"]["fields"]

    # endpoint
    assert "o-field2" in concats_keys

    assert "i-field3" in concats["o-field2"]["fields"]
    assert "i-field4" in concats["o-field2"]["fields"]


def test_lookups_load_csv_success(pipeline_dir):
    """
    test csv loading functionality
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    expected_num_entries = 1
    assert len(lookups.entries) == expected_num_entries


def test_lookups_load_csv_failure(empty_pipeline_dir):
    """
    test csv loading functionality when file absent
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(empty_pipeline_dir)

    with pytest.raises(FileNotFoundError) as fnf_err:
        lookups.load_csv()

    expected_exception = "No such file or directory"
    assert expected_exception in str(fnf_err.value)


def test_load_filter_when_csv_contains_endpoint(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    # create the datasource file used by the Pipeline class
    filter_data = {
        "resource": ["", "", ""],
        "endpoint": ["", test_endpoint, ""],
        "field": ["field1", "field2", "field3"],
        "pattern": ["pattern1", "pattern2", "pattern3"],
    }
    filters_data = pd.DataFrame.from_dict(filter_data)
    filters_data.to_csv(f"{pipeline_dir}/filter.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)

    # -- Assert --
    assert test_endpoint in pipeline.filter
    assert pipeline.filter[test_endpoint]["field2"] == "pattern2"


def test_load_patch_when_csv_contains_endpoint(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    test_pipeline = "test-pipeline"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    # create the datasource file used by the Pipeline class
    patch_data = {
        "resource": ["", "", ""],
        "endpoint": ["", test_endpoint, ""],
        "field": ["field1", "grade", "field2"],
        "pattern": ["pattern1", "^1$", "pattern2"],
        "value": ["value1", "I", "value2"],
    }
    patches_data = pd.DataFrame.from_dict(patch_data)
    patches_data.to_csv(f"{pipeline_dir}/patch.csv", index=False)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    patch = pipeline.patches("test_resource", [test_endpoint])

    # -- Assert --
    assert test_endpoint in pipeline.patch
    assert pipeline.patch[test_endpoint]["grade"] == {"^1$": "I"}
    assert {
        "grade": {"^1$": "I"},
        "field1": {"pattern1": "value1"},
        "field2": {"pattern2": "value2"},
    } in [patch]


def test_load_concat_with_prepend_append(empty_pipeline_dir):
    """
    test the concat data is loaded when the prepend/append fields are present.
    """
    row = {
        "pipeline": "test-pipeline",
        "resource": "",
        "field": "point-field",
        "fields": "field-X;field-Y",
        "separator": " ",
        "prepend": "POINT(",
        "append": ")",
        "entry-date": "2024-09-27",
        "start-date": "2024-09-27",
        "end-date": "",
    }

    with open(os.path.join(empty_pipeline_dir, "concat.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=row.keys())
        dictwriter.writeheader()
        dictwriter.writerow(row)

    pipeline = Pipeline(empty_pipeline_dir, "test-pipeline")

    assert pipeline.concatenations() == {
        "point-field": {
            "fields": ["field-X", "field-Y"],
            "separator": " ",
            "prepend": "POINT(",
            "append": ")",
        }
    }


def test_load_concat_no_prepend_append(empty_pipeline_dir):
    """
    test the concat data is loaded when the prepend/append fields are present.
    """
    row = {
        "pipeline": "test-pipeline",
        "resource": "",
        "field": "test-field",
        "fields": "field-one;field-two",
        "separator": ", ",
        "entry-date": "2024-09-27",
        "start-date": "2024-09-27",
        "end-date": "",
    }

    with open(os.path.join(empty_pipeline_dir, "concat.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=row.keys())
        dictwriter.writeheader()
        dictwriter.writerow(row)

    pipeline = Pipeline(empty_pipeline_dir, "test-pipeline")

    assert pipeline.concatenations() == {
        "test-field": {
            "fields": ["field-one", "field-two"],
            "separator": ", ",
            "prepend": "",
            "append": "",
        }
    }


if __name__ == "__main__":
    pytest.main()
