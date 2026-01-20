import pytest
import os
import csv
import pandas as pd
from digital_land.pipeline import Pipeline
from digital_land.pipeline import Lookups
from digital_land.specification import Specification
from digital_land.organisation import Organisation


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


@pytest.fixture
def organisation_path(tmp_path):
    """Create an organisations dataset for testing."""
    orgs_path = tmp_path / "organisation.csv"
    pd.DataFrame.from_dict(get_test_organisation_data()).to_csv(orgs_path, index=False)
    return orgs_path


def get_test_organisation_data():
    """Test organisation data."""
    return {
        "entity": [101, 102],
        "name": ["Test Org", "Test Org 2"],
        "prefix": ["local-authority", "local-authority"],
        "reference": ["test-org", "test-org-2"],
        "dataset": ["local-authority", "local-authority"],
        "organisation": ["local-authority:LBH", "local-authority:LBH"],
    }


def get_test_resource_data():
    """Test resource data for pipeline transformation."""
    return {
        "WKT": [
            "POLYGON ((-0.1 51.5, -0.1 51.51, -0.09 51.51, -0.09 51.5, -0.1 51.5))",
            "POLYGON ((-0.11 51.5, -0.11 51.51, -0.10 51.51, -0.10 51.5, -0.11 51.5))",
        ],
        "ID": [0, 1],
        "TITLE": ["Test Zone 1", "Test Zone 2"],
    }


def get_test_resource_data_with_unmapped_reference():
    """Test resource data with a reference not in lookup config."""
    return {
        "WKT": [
            "POLYGON ((-0.1 51.5, -0.1 51.51, -0.09 51.51, -0.09 51.5, -0.1 51.5))",
            "POLYGON ((-0.11 51.5, -0.11 51.51, -0.10 51.51, -0.10 51.5, -0.11 51.5))",
            "POLYGON ((-0.12 51.5, -0.12 51.51, -0.11 51.51, -0.11 51.5, -0.12 51.5))",
        ],
        "ID": [0, 1, 99],  # 99 is not in lookup config
        "TITLE": ["Test Zone 1", "Test Zone 2", "Test Zone 3 Unmapped"],
    }


def get_test_column_config(dataset_name):
    """Test column mapping configuration."""
    return {
        "dataset": [dataset_name, dataset_name, dataset_name],
        "resource": ["", "", ""],
        "endpoint": ["", "", ""],
        "column": ["WKT", "ID", "TITLE"],
        "field": ["geometry", "reference", "name"],
    }


def get_test_lookup_config():
    """Test lookup mapping configuration."""
    return {
        "resource": ["", ""],
        "entry-number": ["", ""],
        "prefix": ["central-activities-zone", "central-activities-zone"],
        "reference": ["0", "1"],
        "entity": ["2200001", "2200002"],
        "start-date": ["", ""],
        "organisation": ["101", "101"],
        "end-date": ["", ""],
        "entry-date": ["", ""],
        "endpoint": ["", ""],
    }


@pytest.fixture
def test_resource_file(tmp_path):
    """
    Create a minimal CSV test file for pipeline processing.
    Uses WKT geometry format.
    """
    test_file = tmp_path / "test_resource.csv"
    csv_df = pd.DataFrame.from_dict(get_test_resource_data())
    csv_df.to_csv(test_file, index=False)

    return str(test_file)


def test_pipeline_transform_basic(
    specification_dir, organisation_path, test_resource_file, tmp_path
):
    """
    Lightweight integration test for Pipeline.transform() method.

    Tests that the transform method can successfully:
    - Load CSV input resource with WKT geometry
    - Apply column mappings and transformations
    - Apply organisation defaults
    - Write output in fact-based model format
    - Return an issue log
    """
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    dataset_name = "central-activities-zone"

    # Column mappings: CSV columns -> dataset fields
    pd.DataFrame(get_test_column_config(dataset_name)).to_csv(
        f"{pipeline_dir}/column.csv", index=False
    )

    # Lookup mappings: reference values -> entity IDs
    pd.DataFrame(get_test_lookup_config()).to_csv(
        f"{pipeline_dir}/lookup.csv", index=False
    )

    # Initialize pipeline components
    spec = Specification(specification_dir)
    org = Organisation(organisation_path=organisation_path)
    pipeline = Pipeline(str(pipeline_dir), dataset_name, specification=spec)

    output_path = tmp_path / "output" / "transformed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # -- Act --
    issue_log = pipeline.transform(
        input_path=test_resource_file,
        output_path=output_path,
        organisation=org,
        resource=test_resource_file,
        valid_category_values={},
        organisations=["local-authority:LBH"],
        disable_lookups=False,
    )

    # -- Assert --
    assert output_path.exists(), "Output file should be created"
    assert issue_log is not None, "Issue log should be returned"

    # Verify output structure and content
    output_df = pd.read_csv(output_path)
    assert len(output_df) > 0, "Output should contain at least one row"
    assert "entity" in output_df.columns, "Output should have entity column"
    assert "field" in output_df.columns, "Output should have field column"
    assert "value" in output_df.columns, "Output should have value column"

    # Verify expected fields are present
    actual_fields = set(output_df["field"].unique())
    expected_fields = {"prefix", "geometry", "reference", "name", "organisation"}
    assert expected_fields.issubset(
        actual_fields
    ), f"Expected fields {expected_fields} not all present in {actual_fields}"

    # Verify entities were created
    assert output_df["entity"].notna().any(), "At least one entity should be created"
    entities = output_df["entity"].dropna().unique()
    assert len(entities) > 0, "Should have created entities for test data"


def test_pipeline_transform_with_unmapped_reference_lookup_enabled(
    specification_dir, organisation_path, tmp_path
):
    """
    Test that when lookups are enabled and data has unmapped references,
    issue log contains 'unknown entity - missing reference' errors.
    """
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    dataset_name = "central-activities-zone"

    # Create test resource with unmapped reference
    test_file = tmp_path / "test_resource.csv"
    pd.DataFrame(get_test_resource_data_with_unmapped_reference()).to_csv(
        test_file, index=False
    )

    # Column mappings
    pd.DataFrame(get_test_column_config(dataset_name)).to_csv(
        f"{pipeline_dir}/column.csv", index=False
    )

    # Lookup mappings (only references 0 and 1, not 99)
    pd.DataFrame(get_test_lookup_config()).to_csv(
        f"{pipeline_dir}/lookup.csv", index=False
    )

    spec = Specification(specification_dir)
    org = Organisation(organisation_path=organisation_path)
    pipeline = Pipeline(str(pipeline_dir), dataset_name, specification=spec)

    output_path = tmp_path / "output" / "transformed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # -- Act --
    issue_log = pipeline.transform(
        input_path=str(test_file),
        output_path=output_path,
        organisation=org,
        resource=str(test_file),
        valid_category_values={},
        organisations=["local-authority:LBH"],
        disable_lookups=False,  # Lookups ENABLED
    )

    # -- Assert --
    assert output_path.exists(), "Output file should be created"

    # Check that issue log contains "unknown entity - missing reference"
    issues = issue_log.rows
    issue_types = [issue["issue-type"] for issue in issues if "issue-type" in issue]
    assert (
        "unknown entity" in issue_types
    ), "Should have 'unknown entity' error for unmapped reference 99"

    # Verify the unmapped reference (99) was NOT included in output (pruned)
    output_df = pd.read_csv(output_path)
    # When lookups are enabled, unmapped references get pruned, so we should only have 2 entities
    entities = output_df["entity"].dropna().unique()
    assert len(entities) == 2, "Should only have 2 entities (reference 99 pruned)"


def test_pipeline_transform_with_unmapped_reference_lookup_disabled(
    specification_dir, organisation_path, tmp_path
):
    """
    Test that when lookups are disabled and data has unmapped references,
    issue log does NOT contain 'unknown entity - missing reference' errors,
    and data is created with empty entity column.
    """
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    dataset_name = "central-activities-zone"

    # Create test resource with unmapped reference
    test_file = tmp_path / "test_resource.csv"
    pd.DataFrame(get_test_resource_data_with_unmapped_reference()).to_csv(
        test_file, index=False
    )

    # Column mappings
    pd.DataFrame(get_test_column_config(dataset_name)).to_csv(
        f"{pipeline_dir}/column.csv", index=False
    )

    # Lookup mappings (only references 0 and 1, not 99)
    pd.DataFrame(get_test_lookup_config()).to_csv(
        f"{pipeline_dir}/lookup.csv", index=False
    )

    spec = Specification(specification_dir)
    org = Organisation(organisation_path=organisation_path)
    pipeline = Pipeline(str(pipeline_dir), dataset_name, specification=spec)

    output_path = tmp_path / "output" / "transformed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # -- Act --
    issue_log = pipeline.transform(
        input_path=str(test_file),
        output_path=output_path,
        organisation=org,
        resource=str(test_file),
        valid_category_values={},
        organisations=["local-authority:LBH"],
        disable_lookups=True,  # Lookups DISABLED
    )

    # -- Assert --
    assert output_path.exists(), "Output file should be created"

    # Check that issue log does NOT contain "unknown entity - missing reference"
    issues = issue_log.rows
    issue_types = [issue["issue-type"] for issue in issues if "issue-type" in issue]
    assert (
        "unknown entity - missing reference" not in issue_types
    ), "Should NOT have 'unknown entity - missing reference' error when lookups disabled"

    # Verify all data was created including unmapped reference
    output_df = pd.read_csv(output_path)

    # Check that we have rows with empty entity values (the unmapped reference)
    empty_entity_rows = output_df[
        output_df["entity"].isna() | (output_df["entity"] == "")
    ]
    assert (
        len(empty_entity_rows) > 0
    ), "Should have rows with empty entity for unmapped reference 99"

    # Verify reference field for unmapped data exists in output
    reference_values = output_df[output_df["field"] == "reference"]["value"].tolist()
    assert "99" in reference_values, "Unmapped reference 99 should be in output"


if __name__ == "__main__":
    pytest.main()
