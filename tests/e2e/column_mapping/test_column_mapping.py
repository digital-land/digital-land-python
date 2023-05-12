import os
import urllib.request
import pandas as pd

import pytest

from pathlib import Path
from urllib.error import URLError

from digital_land.log import ColumnFieldLog
from digital_land.phase.map import MapPhase
from digital_land.pipeline import Pipeline, run_pipeline
from digital_land.specification import Specification

from io import StringIO
from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase


def get_pipeline_csv_data_with_resources_and_endpoints(
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
            "res-col-one",
            "ep-col-one",
        ],
        "field": [
            "name",
            "res_field_one",
            "ep_field_one",
        ],
    }


def copy_latest_specification_files_to(specification_dir: Path | None = None):
    error_msg = "Failed to download specification files"
    if not specification_dir:
        pytest.fail(error_msg)

    url_domain = "https://raw.githubusercontent.com"
    url_path = "/digital-land/specification/main/specification"
    specification_url = f"{url_domain}{url_path}"
    specification_csv_list = [
        "attribution.csv",
        "collection.csv",
        "datapackage-dataset.csv",
        "datapackage.csv",
        "dataset-field.csv",
        "dataset-schema.csv",
        "dataset.csv",
        "datatype.csv",
        "field.csv",
        "issue-type.csv",
        "licence.csv",
        "organisation-dataset.csv",
        "pipeline.csv",
        "prefix.csv",
        "project-status.csv",
        "project.csv",
        "provision-reason.csv",
        "schema-field.csv",
        "schema.csv",
        "severity.csv",
        "specification-status.csv",
        "specification.csv",
        "theme.csv",
        "typology.csv",
    ]

    try:
        for specification_csv in specification_csv_list:
            urllib.request.urlretrieve(
                f"{specification_url}/{specification_csv}",
                os.path.join(specification_dir, specification_csv),
            )
    except URLError:
        pytest.fail(error_msg)


def create_inputs_stream_from_dict(inputs: dict | None = None):
    separator = ","
    keys_str_list = separator.join(list(inputs.keys()))
    values_str_list = separator.join(list(inputs.values()))

    return StringIO(f"{keys_str_list}\r\n{values_str_list}")


def apply_pipeline_transforms(phases: list | None = None):
    error_msg_1 = "No phases were provided to apply transforms to."

    if not phases:
        pytest.fail(error_msg_1)

    run_pipeline(*phases)


def test_pipeline_with_map_phase(tmp_path):
    # -- Arrange --
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    specification_dir = tmp_path / "specification"
    specification_dir.mkdir()

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_pipeline = "national-park"
    test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    # create the datasource file used by the Pipeline class
    raw_data = get_pipeline_csv_data_with_resources_and_endpoints(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # create the datasource files used by the Specification class
    copy_latest_specification_files_to(specification_dir)

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    specification = Specification(specification_dir)
    column_field_log = ColumnFieldLog(dataset=test_pipeline, resource=test_resource)

    columns = pipeline.columns(test_resource, test_endpoints)
    fieldnames = specification.intermediate_fieldnames(pipeline)

    # initialise transform phases
    inputs_stream = create_inputs_stream_from_dict(columns)

    load_phase = LoadPhase(f=inputs_stream)

    parse_phase = ParsePhase()

    map_phase = MapPhase(
        fieldnames=fieldnames,
        columns=columns,
        log=column_field_log,
    )

    apply_pipeline_transforms(
        [
            load_phase,
            parse_phase,
            map_phase,
        ]
    )

    output_file = f"{output_dir}/{test_resource}.csv"
    column_field_log.save(output_file)

    # -- Asert --
    df = pd.read_csv(output_file, index_col=False)

    assert "ep-col-one" in df["column"].values
    assert "name" in df["column"].values
    assert "res-col-one" in df["column"].values

    assert "ep_field_one" in df["field"].values
    assert "name" in df["field"].values
    assert "res_field_one" in df["field"].values
