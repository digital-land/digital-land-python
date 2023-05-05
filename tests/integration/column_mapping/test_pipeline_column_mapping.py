import shutil
from pathlib import Path

from digital_land.log import ColumnFieldLog
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification

from tests.integration.column_mapping.helper.funcs import prepare_data, process_pipeline

import pytest


@pytest.fixture
def env_vars(tmp_path, request):
    root_dir = request.config.rootdir
    test_root = Path(root_dir).resolve()

    target_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
    endpoints = [dummy_endpoint, target_endpoint, dummy_endpoint[::-1]]

    return {
        "endpoints": endpoints,
        "resource": "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
        "dataset_name": "article-4-direction-area",
        "column_all_csv": f"{tmp_path}/data/column_all.csv",
        "column_res_csv": f"{tmp_path}/data/column_res.csv",
        "column_eps_csv": f"{tmp_path}/data/column_eps.csv",
        "column_csv": f"{tmp_path}/pipeline/column.csv",
        "pipeline_dir": f"{tmp_path}/pipeline/",
        "specification_dir": f"{test_root}/tests/integration/column_mapping/specification/",
        "output_dir": f"{tmp_path}/output/",
    }


@pytest.fixture(autouse=True)
def before_and_after_tests(tmp_path):
    """Fixture to execute asserts before and after a test is run"""
    # Setup:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    yield

    # Teardown:


def test_required_external_asset_creation(env_vars):
    columns_all_csv = env_vars["column_all_csv"]
    columns_csv = env_vars["column_csv"]
    pipeline_dir = env_vars["pipeline_dir"]
    dataset = env_vars["dataset_name"]
    resource = env_vars["resource"]

    # -- Arrange --
    prepare_data(columns_all_csv)

    shutil.copyfile(columns_all_csv, columns_csv)

    # create required components
    pipeline = Pipeline(pipeline_dir, dataset)

    specification_dir = env_vars["specification_dir"]
    specification = Specification(specification_dir)

    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # -- Assert --
    assert pipeline.name == dataset
    assert pipeline.dataset == dataset

    assert len(specification.dataset) != 0
    assert len(specification.dataset_names) != 0
    assert len(specification.dataset_schema) != 0
    assert len(specification.datatype) != 0
    assert len(specification.datatype_names) != 0
    assert len(specification.field) != 0
    assert len(specification.field_names) != 0
    assert len(specification.field_schema) != 0
    assert len(specification.pipeline) != 0

    assert column_field_log.dataset is dataset
    assert column_field_log.resource is resource


def test_all_resources_no_endpoints(env_vars):
    columns_res_csv = env_vars["column_res_csv"]
    columns_csv = env_vars["column_csv"]
    pipeline_dir = env_vars["pipeline_dir"]
    dataset = env_vars["dataset_name"]
    resource = env_vars["resource"]
    endpoints = env_vars["endpoints"]
    output_dir = env_vars["output_dir"]

    # -- Arrange --
    prepare_data(columns_res_csv)
    shutil.copyfile(columns_res_csv, columns_csv)

    # create required components
    pipeline = Pipeline(pipeline_dir, dataset)

    specification_dir = env_vars["specification_dir"]
    specification = Specification(specification_dir)

    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # -- Act --
    params = {
        "pipeline": pipeline,
        "specification": specification,
        "column_field_log": column_field_log,
        "endpoints": endpoints,
        "resource": resource,
        "output_dir": output_dir,
    }
    output = process_pipeline(params)

    # -- Assert --
    # expected empty outputs
    assert output[0]["row"]["address-text"] == ""
    assert output[0]["row"]["document-url"] == ""
    assert output[0]["row"]["organisation"] == ""

    # expected outputs from resource
    assert "res_field_one" in output[0]["row"]
    assert "res_field_two" in output[0]["row"]
    assert "res_field_three" in output[0]["row"]

    # expected outputs from endpoints
    assert "ep_field_one" not in output[0]["row"]
    assert "ep_field_teo" not in output[0]["row"]
    assert "ep_field_three" not in output[0]["row"]


def test_all_endpoints_no_resources(env_vars):
    columns_eps_csv = env_vars["column_eps_csv"]
    columns_csv = env_vars["column_csv"]
    pipeline_dir = env_vars["pipeline_dir"]
    dataset = env_vars["dataset_name"]
    resource = env_vars["resource"]
    endpoints = env_vars["endpoints"]
    output_dir = env_vars["output_dir"]

    # -- Arrange --
    prepare_data(columns_eps_csv)
    shutil.copyfile(columns_eps_csv, columns_csv)

    # create required components
    pipeline = Pipeline(pipeline_dir, dataset)

    specification_dir = env_vars["specification_dir"]
    specification = Specification(specification_dir)

    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # -- Act --
    params = {
        "pipeline": pipeline,
        "specification": specification,
        "column_field_log": column_field_log,
        "endpoints": endpoints,
        "resource": resource,
        "output_dir": output_dir,
    }
    output = process_pipeline(params)

    # -- Assert --
    # expected empty outputs
    assert output[0]["row"]["address-text"] == ""
    assert output[0]["row"]["document-url"] == ""
    assert output[0]["row"]["organisation"] == ""

    # expected outputs from resource
    assert "res_field_one" not in output[0]["row"]
    assert "res_field_two" not in output[0]["row"]
    assert "res_field_three" not in output[0]["row"]

    # expected outputs from endpoints
    assert "ep_field_one" in output[0]["row"]
    assert "ep_field_two" in output[0]["row"]
    assert "ep_field_three" in output[0]["row"]


def test_resources_and_endpoints(tmp_path, env_vars):
    columns_all_csv = env_vars["column_all_csv"]
    columns_csv = env_vars["column_csv"]
    pipeline_dir = env_vars["pipeline_dir"]
    dataset = env_vars["dataset_name"]
    resource = env_vars["resource"]
    endpoints = env_vars["endpoints"]
    output_dir = env_vars["output_dir"]

    # -- Arrange --
    prepare_data(columns_all_csv)
    shutil.copyfile(columns_all_csv, columns_csv)

    # create required components
    pipeline = Pipeline(pipeline_dir, dataset)

    specification_dir = env_vars["specification_dir"]
    specification = Specification(specification_dir)

    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # -- Act --
    params = {
        "pipeline": pipeline,
        "specification": specification,
        "column_field_log": column_field_log,
        "endpoints": endpoints,
        "resource": resource,
        "output_dir": output_dir,
    }
    output = process_pipeline(params)

    # -- Assert --
    # expected empty outputs
    assert output[0]["row"]["address-text"] == ""
    assert output[0]["row"]["document-url"] == ""
    assert output[0]["row"]["organisation"] == ""

    # expected outputs from resource
    assert "res_field_one" in output[0]["row"]
    assert "res_field_two" in output[0]["row"]
    assert "res_field_three" in output[0]["row"]

    # expected outputs from endpoints
    assert "ep_field_one" in output[0]["row"]
    assert "ep_field_two" in output[0]["row"]
    assert "ep_field_three" in output[0]["row"]
