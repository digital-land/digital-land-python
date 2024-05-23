import pytest
import pandas as pd
from digital_land.pipeline import Pipeline


@pytest.fixture(scope="session")
def test_pipeline(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline", numbered=False)

    filter_data = {
        "dataset": ["test-dataset", "test-dataset"],
        "resource": ["test-resource", ""],
        "endpoint": ["", "test-endpoint"],
        "field": ["test-field1", "test-field2"],
        "pattern": ["pattern1", "pattern2"],
    }
    filter_df = pd.DataFrame.from_dict(filter_data)
    filter_df.to_csv(f"{pipeline_dir}/filter.csv", index=False)

    return Pipeline(str(pipeline_dir), "test-dataset")


def test_apply_filter_against_endpoint(test_pipeline):
    filters = test_pipeline.filters(endpoints="test-endpoint")
    assert filters["test-field2"] == "pattern2"


def test_error_on_multiple_patterns_for_same_field_and_endpoint(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline_error", numbered=False)

    filter_data = {
        "dataset": ["test-dataset", "test-dataset"],
        "resource": ["", ""],
        "endpoint": ["test-endpoint", "test-endpoint"],
        "field": ["test-field", "test-field"],
        "pattern": ["pattern1", "pattern2"],
    }
    filter_df = pd.DataFrame.from_dict(filter_data)
    filter_df.to_csv(f"{pipeline_dir}/filter.csv", index=False)

    pipeline = Pipeline(str(pipeline_dir), "test-dataset")
    filters = pipeline.filters(endpoints="test-endpoint")
    print(f"Filters for test-endpoint with conflicting patterns: {filters}")
    assert "test-field" in filters
    assert filters["test-field"] in ["pattern1", "pattern2"]


def test_apply_different_filter_to_resource(test_pipeline):
    filters = test_pipeline.filters(resource="test-resource")
    assert filters["test-field1"] == "pattern1"
