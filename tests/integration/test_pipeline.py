import pandas as pd
from digital_land.pipeline import Pipeline


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
            "res_field_one",
            "ep_field_one",
        ],
    }


def get_csv_data_with_resource_endpoint_clash_ep_first(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    """
    This function generates data to produce a valid column.csv file with clashing definitions
    defined by resources and endpoints. The output file will have the endpoint column mapping
    definition appear first in the column.csv file
    :param pipeline:
    :param resource:
    :param endpoint:
    :return: dict
    """

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
        "field": ["name", "ep_ref", "res_ref"],
    }


def get_csv_data_with_resource_endpoint_clash_res_first(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    """
    This function generates data to produce a valid column.csv file with clashing definitions
    defined by resources and endpoints. The output file will have the resource column mapping
    definition appear first in the column.csv file
    :param pipeline:
    :param resource:
    :param endpoint:
    :return: dict
    """
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
        "field": ["name", "res_ref", "ep_ref"],
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
    p = Pipeline(pipeline_dir, test_pipeline)

    # -- Asert --
    column_keys = list(p.column.keys())
    column_values = list(p.column.values())

    assert test_resource in column_keys
    assert test_endpoint in column_keys

    assert {"name": "name"} in column_values
    assert {"objectid": "res_field_one"} in column_values
    assert {"dummy_fiel": "ep_field_one"} in column_values


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
    p = Pipeline(pipeline_dir, test_pipeline)

    # -- Asert --
    concat_keys = list(p.concat.keys())
    concat_values = list(p.concat.values())

    assert test_resource in concat_keys
    assert test_endpoint in concat_keys

    assert "o-field1" in concat_values[0].keys()
    assert "o-field2" in concat_values[1].keys()
    # when a resource and endpoint are present in a concat data row, resource takes priority
    assert "o-field3" in concat_values[0].keys()

    assert "i-field1" in concat_values[0]["o-field1"]["fields"]
    assert "i-field2" in concat_values[0]["o-field1"]["fields"]

    assert "i-field3" in concat_values[1]["o-field2"]["fields"]
    assert "i-field4" in concat_values[1]["o-field2"]["fields"]

    assert "i-field5" in concat_values[0]["o-field3"]["fields"]
    assert "i-field6" in concat_values[0]["o-field3"]["fields"]

    assert "o-field3" not in concat_values[1].keys()


#
#
# def test_columns_when_csv_contains_endpoints_and_resources(tmp_path):
#     # -- Arrange --
#     pipeline_dir = tmp_path / "pipeline"
#     pipeline_dir.mkdir()
#
#     dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
#
#     test_pipeline = "test-pipeline"
#     test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
#     test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
#     test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]
#
#     raw_data = get_csv_data_for_resources_and_endpoints(
#         test_pipeline, test_resource, test_endpoint
#     )
#     columns_data = pd.DataFrame.from_dict(raw_data)
#     columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)
#
#     # -- Act --
#     p = Pipeline(pipeline_dir, test_pipeline)
#     columns = p.columns(test_resource, test_endpoints)
#
#     # -- Asert --
#     assert columns["name"] == "name"
#     assert columns["objectid"] == "res_field_one"
#     assert columns["dummy_fiel"] == "ep_field_one"
#
#
# def test_columns_when_csv_contains_only_resources(tmp_path):
#     # -- Arrange --
#     pipeline_dir = tmp_path / "pipeline"
#     pipeline_dir.mkdir()
#
#     dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
#
#     test_pipeline = "test-pipeline"
#     test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
#     test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
#     test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]
#
#     raw_data = get_csv_data_for_resources_only(test_pipeline, test_resource)
#     columns_data = pd.DataFrame.from_dict(raw_data)
#     columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)
#
#     # -- Act --
#     p = Pipeline(pipeline_dir, test_pipeline)
#     columns = p.columns(test_resource, test_endpoints)
#
#     # -- Asert --
#     assert columns["name"] == "name"
#     assert columns["objectid"] == "res_field_one"
#     assert "dummy_fiel" not in columns
#
#
# def test_columns_when_csv_contains_only_endpoints(tmp_path):
#     # -- Arrange --
#     pipeline_dir = tmp_path / "pipeline"
#     pipeline_dir.mkdir()
#
#     dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
#
#     test_pipeline = "test-pipeline"
#     test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
#     test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
#     test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]
#
#     raw_data = get_csv_data_for_endpoints_only(test_pipeline, test_endpoint)
#     columns_data = pd.DataFrame.from_dict(raw_data)
#     columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)
#
#     # -- Act --
#     p = Pipeline(pipeline_dir, test_pipeline)
#     columns = p.columns(test_resource, test_endpoints)
#
#     # -- Asert --
#     assert columns["name"] == "name"
#     assert columns["dummy_fiel"] == "ep_field_one"
#     assert "res_field_one" not in columns
#
#
# def test_columns_when_csv_contains_clashing_entries_res_first(tmp_path):
#     """
#     Check that resource column mapping definitions take priority over
#     endpoint column mapping definitions.
#     This test ensures correct functionality when the resource column mapping
#     definition appears first in the column.csv file
#     :param tmp_path:
#     :return: None
#     """
#     # -- Arrange --
#     pipeline_dir = tmp_path / "pipeline"
#     pipeline_dir.mkdir()
#
#     test_pipeline = "test-pipeline"
#     test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
#     test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
#     dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
#     test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]
#
#     raw_data = get_csv_data_with_resource_endpoint_clash_res_first(
#         test_pipeline, test_resource, test_endpoint
#     )
#     columns_data = pd.DataFrame.from_dict(raw_data)
#     columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)
#
#     # -- Act --
#     p = Pipeline(pipeline_dir, test_pipeline)
#     columns = p.columns(test_resource, test_endpoints)
#
#     # -- Asert --
#     assert columns["name"] == "name"
#     assert columns["ref"] == "res_ref"
#     assert "ep_ref" not in columns
#
#
# def test_columns_when_csv_contains_clashing_entries_ep_first(tmp_path):
#     """
#     Check that resource column mapping definitions take priority over
#     endpoint column mapping definitions.
#     This test ensures correct functionality when the endpoint column mapping
#     definition appears first in the column.csv file
#     :param tmp_path:
#     :return: None
#     """
#     # -- Arrange --
#     pipeline_dir = tmp_path / "pipeline"
#     pipeline_dir.mkdir()
#
#     test_pipeline = "test-pipeline"
#     test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
#     test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
#     dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
#     test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]
#
#     raw_data = get_csv_data_with_resource_endpoint_clash_ep_first(
#         test_pipeline, test_resource, test_endpoint
#     )
#     columns_data = pd.DataFrame.from_dict(raw_data)
#     columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)
#
#     # -- Act --
#     p = Pipeline(pipeline_dir, test_pipeline)
#     columns = p.columns(test_resource, test_endpoints)
#
#     # -- Asert --
#     assert columns["name"] == "name"
#     assert columns["ref"] == "res_ref"
#     assert "ep_ref" not in columns
