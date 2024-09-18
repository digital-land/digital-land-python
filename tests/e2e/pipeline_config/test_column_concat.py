import os
import urllib.request
from io import StringIO
from pathlib import Path
from urllib.error import URLError

import pandas as pd

from digital_land.phase.concat import ConcatFieldPhase
from digital_land.commands import pipeline_run
from digital_land.pipeline import Pipeline, run_pipeline
from digital_land.specification import Specification
from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.save import SavePhase

import pytest

test_pipeline = "national-park"
test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"


@pytest.fixture(scope="session")
def test_dirs(tmp_path_factory):
    """
    This fixture prepares the folders and data required by the tests that use it.
    Some of the data is retrieved remotely, so it makes sense to retrieve once only
    for all tests.
    :param tmp_path_factory:
    :return: dict of Path (directory paths)
    """

    # directories
    pipeline_dir = tmp_path_factory.mktemp("concat_pipeline", numbered=False)

    specification_dir = tmp_path_factory.mktemp("concat_specification", numbered=False)

    collection_dir = tmp_path_factory.mktemp("concat_collection", numbered=False)
    issues_log_dir = tmp_path_factory.mktemp("concat_issues-log", numbered=False)
    dataaset_resource_dir = tmp_path_factory.mktemp(
        "concat_dataset-resource", numbered=False
    )
    converted_resource_dir = tmp_path_factory.mktemp(
        "concat_converted-resource", numbered=False
    )

    output_dir = tmp_path_factory.mktemp("concat_output", numbered=False)

    # data - pipeline
    raw_data = get_test_concat_csv_data_with_endpoints_and_resources(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/concat.csv", index=False)

    # data - specification
    copy_latest_specification_files_to(specification_dir)

    # data - collection
    generate_test_collection_files(
        str(collection_dir), test_pipeline, test_resource, test_endpoint
    )

    return {
        "collection_dir": collection_dir,
        "output_dir": output_dir,
        "pipeline_dir": pipeline_dir,
        "specification_dir": specification_dir,
        "issues_log_dir": issues_log_dir,
        "dataset_resource_dir": dataaset_resource_dir,
        "converted_resource_dir": converted_resource_dir,
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


def create_inputs_stream_from_dict(inputs: dict):
    separator = ","
    field_str = ""
    value_str = ""
    for key, value in inputs.items():
        field_str += "," + separator.join(value["fields"])
        if key == "o-field1":
            value_str += "i-value1,i-value2"
        if key == "o-field2":
            value_str += ",i-value3,i-value4"
        if key == "o-field3":
            value_str += ",i-value5,i-value6"

    field_str = field_str[1:]

    return StringIO(f"{field_str}\r\n{value_str}\r\n")
    # return StringIO("i-field1,i-field2\r\ni-value1,i-value2\r\n")


def apply_pipeline_transforms(phases: list):
    error_msg_1 = "No phases were provided to apply transforms to."

    if not phases:
        pytest.fail(error_msg_1)

    run_pipeline(*phases)


def generate_test_collection_files(
    collection_dir: str = "", pipeline: str = "", resource: str = "", endpoint: str = ""
):
    collection_csv_list = [
        "endpoint.csv",
        "source.csv",
        "resource.csv",
        f"{resource}.csv",
    ]

    for collection_csv in collection_csv_list:
        if collection_csv == "endpoint.csv":
            raw_data = {
                "endpoint": [endpoint],
                "endpoint-url": ["https://example.com/register/1.csv"],
                "start-date": ["2020-01-12"],
                "end-date": ["2020-02-01"],
            }

        if collection_csv == "source.csv":
            raw_data = {
                "source": [""],
                "collection": ["test-collection"],
                "pipeline": [pipeline],
                "organisation": ["test-org"],
                "endpoint": [endpoint],
                "documentation-url": [""],
                "licence": [""],
                "attribution": [""],
                "start-date": [""],
                "end-date": [""],
            }

        if collection_csv == "resource.csv":
            raw_data = {
                "resource": [resource, ""],
                "bytes": [1234, 1234],
                "organisations": ["test-org", "test-org"],
                "datasets": [pipeline, pipeline],
                "endpoints": ["", endpoint],
                "res-col-one": ["res-col-val", ""],
                "ep-col-one": ["", "ep-col-val"],
                "start-date": ["2020-01-12", "2020-01-12"],
                "end-date": ["2020-02-01", "2020-02-01"],
            }

        if collection_csv.startswith(resource):
            raw_data = {
                "resource": [resource, ""],
                "bytes": [1234, 1234],
                "organisations": ["test-org", "test-org"],
                "datasets": [pipeline, pipeline],
                "endpoints": ["", endpoint],
                "res-col-one": ["res-col-val", ""],
                "ep-col-one": ["", "ep-col-val"],
                "start-date": ["2020-01-12", "2020-01-12"],
                "end-date": ["2020-02-01", "2020-02-01"],
            }

        columns_data = pd.DataFrame.from_dict(raw_data)
        columns_data.to_csv(f"{collection_dir}/{collection_csv}", index=False)


def copy_latest_specification_files_to(specification_dir: Path):
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


def test_run_pipeline_with_concat_field_phase(test_dirs):
    """
    This test uses pipeline.py run_pipeline to perform a run-through
    of the selected pipeline phases, using test data generated by the input
    fixture test_dirs, which itself uses the global vars:
    test_pipeline
    test_resource
    test_endpoint
    :param test_dirs:
    :return: None
    """
    # -- Arrange --
    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"

    pipeline_dir = test_dirs["pipeline_dir"]

    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    # -- Act --
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    concats = pipeline.concatenations(test_resource, test_endpoints)

    # initialise transform phases
    inputs_stream = create_inputs_stream_from_dict(concats)  # TODO

    load_phase = LoadPhase(f=inputs_stream)

    parse_phase = ParsePhase()

    concat_field_phase = ConcatFieldPhase(concats=concats, log=None)

    output = StringIO()
    save_phase = SavePhase(f=output)

    apply_pipeline_transforms([load_phase, parse_phase, concat_field_phase, save_phase])

    concat_results = output.getvalue().split("\r\n")
    fields_list = concat_results[0].split(",")
    values_list = concat_results[1].split(",")

    assert "i-field1" in fields_list
    assert "i-field2" in fields_list
    assert "i-field3" in fields_list
    assert "i-field4" in fields_list
    assert "i-field5" in fields_list
    assert "i-field6" in fields_list
    assert "o-field1" in fields_list
    assert "o-field2" in fields_list
    assert "o-field3" in fields_list

    assert "i-value1" in values_list
    assert "i-value2" in values_list
    assert "i-value3" in values_list
    assert "i-value4" in values_list
    assert "i-value5" in values_list
    assert "i-value6" in values_list

    assert "i-value1.i-value2" in values_list
    assert "i-value3.i-value4" in values_list
    assert "i-value5.i-value6" in values_list


def test_pipeline_run(test_dirs):
    """
    This test uses commands.py pipeline_run to perform a run-through
    of all the pipeline phases, using test data generated by the input
    fixture test_dirs, which itself uses the global vars:
    test_pipeline
    test_resource
    test_endpoint
    ***
    Concatenations are not output by any phase of pipeline_run.
    This test ensures that endpoints are being sent to pipeline.concatenations
     via the pipeline_run function parameters.
    The pipeline.concat field should have the concatenations defined by both
    resources and endpoints defined in the test files
    ***
    :param test_dirs:
    :return: None
    """

    # -- Arrange --
    dummy_endpoint = "abcde12345fghij67890abcde12345fghij67890abcde12345fghij67890abcd"

    pipeline_dir = test_dirs["pipeline_dir"]
    specification_dir = test_dirs["specification_dir"]

    pipeline = Pipeline(pipeline_dir, test_pipeline)
    specification = Specification(specification_dir)
    input_path = test_dirs["collection_dir"] / f"{test_resource}.csv"
    output_path = test_dirs["pipeline_dir"] / f"{test_resource}.csv"
    collection_dir = test_dirs["collection_dir"]
    issue_dir = test_dirs["issues_log_dir"]
    organisation_path = "tests/data/listed-building/organisation.csv"
    dataset_resource_dir = test_dirs["dataset_resource_dir"]
    converted_resource_dir = test_dirs["converted_resource_dir"]
    test_endpoints = [dummy_endpoint, test_endpoint, dummy_endpoint[::-1]]

    # -- Act --
    pipeline_run(
        dataset=test_pipeline,
        pipeline=pipeline,
        specification=specification,
        input_path=input_path,
        output_path=output_path,
        collection_dir=collection_dir,  # TBD: remove, replaced by endpoints, organisations and entry_date
        null_path=None,  # TBD: remove this
        issue_dir=issue_dir,
        organisation_path=organisation_path,
        save_harmonised=False,
        column_field_dir=pipeline_dir,
        dataset_resource_dir=dataset_resource_dir,
        converted_resource_dir=converted_resource_dir,
        custom_temp_dir=None,  # TBD: rename to "tmpdir"
        endpoints=test_endpoints,
        organisations=[],
        entry_date="",
    )

    resource_concat_def = pipeline.concat.get(test_resource, {})
    endpoint_concat_def = pipeline.concat.get(test_endpoint, {})

    # resource
    assert "o-field1" in resource_concat_def.keys()
    assert "o-field3" in resource_concat_def.keys()

    assert "i-field1" in resource_concat_def["o-field1"]["fields"]
    assert "i-field2" in resource_concat_def["o-field1"]["fields"]
    assert "i-field5" in resource_concat_def["o-field3"]["fields"]
    assert "i-field6" in resource_concat_def["o-field3"]["fields"]

    # endpoint
    assert "o-field2" in endpoint_concat_def.keys()

    assert "i-field3" in endpoint_concat_def["o-field2"]["fields"]
    assert "i-field4" in endpoint_concat_def["o-field2"]["fields"]
