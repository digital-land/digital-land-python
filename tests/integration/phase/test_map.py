import os
import shutil
from pathlib import Path
import pandas as pd

from digital_land.log import ColumnFieldLog
from digital_land.phase.map import MapPhase
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification

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
        "specification_dir": f"{test_root}/tests/data/specification-latest/",
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
    pass


def get_columns_csv_data_all():
    return {
        "dataset": [
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
        ],
        "resource": [
            "",
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
            "",
        ],
        "endpoint": [
            "",
            "",
            "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412",
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


def get_columns_csv_data_res():
    return {
        "dataset": [
            "article-4-direction-area",
            "article-4-direction-area",
        ],
        "resource": [
            "",
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
        ],
        "endpoint": ["", ""],
        "column": [
            "NAME",
            "OBJECTID",
        ],
        "field": [
            "name",
            "res_field_one",
        ],
    }


def get_columns_csv_data_eps():
    return {
        "dataset": [
            "article-4-direction-area",
            "article-4-direction-area",
        ],
        "resource": ["", ""],
        "endpoint": [
            "",
            "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412",
        ],
        "column": [
            "NAME",
            "dummy_fiel",
        ],
        "field": [
            "name",
            "ep_field_one",
        ],
    }


def prepare_data(data_src: str = ""):
    data_selector = data_src.split("/")[-1].split(".")[0]

    if data_selector == "column_eps":
        csv_data = get_columns_csv_data_eps()
    elif data_selector == "column_res":
        csv_data = get_columns_csv_data_res()
    else:
        csv_data = get_columns_csv_data_all()

    columns_data = pd.DataFrame.from_dict(csv_data)
    columns_data.to_csv(data_src, index=False)


def process_pipeline(params=None):
    if params is None:
        params = {}
    resource = params["resource"]
    pipeline = params["pipeline"]
    endpoints = params["endpoints"]
    specification = params["specification"]
    column_field_log = params["column_field_log"]
    output_dir = params["output_dir"]

    columns = pipeline.columns(resource, endpoints)
    fieldnames = specification.intermediate_fieldnames(pipeline)
    log = column_field_log

    mock_input_stream = [
        {
            "row": columns,
            "entry-number": 1,
        }
    ]

    map_phase = MapPhase(
        fieldnames=fieldnames,
        columns=columns,
        log=column_field_log,
    )

    output = [block for block in map_phase.process(mock_input_stream)]

    log.save(os.path.join(output_dir, resource + ".csv"))

    return output


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

    # expected outputs from endpoints
    assert "ep_field_one" not in output[0]["row"]


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

    # expected outputs from endpoints
    assert "ep_field_one" in output[0]["row"]


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

    # expected outputs from endpoints
    assert "ep_field_one" in output[0]["row"]
