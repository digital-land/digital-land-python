import os
from pathlib import Path
import shutil

import pytest

from digital_land.commands import resource_from_path
from digital_land.log import ColumnFieldLog
from digital_land.phase.map import MapPhase
from digital_land.pipeline import Pipeline, run_pipeline
from digital_land.specification import Specification
# from tests.e2e.column_mapping.helper.funcs import run_pipeline_test


def pytest_namespace():
    return {
        "fieldnames": [],
        "columns": [],
        "log": None
    }

@pytest.fixture
def env_vars(request):
    root_dir = request.config.rootdir
    tests_dir = Path(root_dir).resolve()

    return {
        "organisation": "local-authority-eng:SWK",
        "pipeline_name": "article-4-direction-area",
        "dataset_name": "article-4-direction",
        "endpoint_path": f"{tests_dir}/tests/data/collection-article-4-direction/",
        "collection_dir": f"{tests_dir}/tests/data/collection-article-4-direction/",
        "test_dir_out": f"{root_dir}/tests/e2e/column_mapping/output/",
        "specification_dir": f"{tests_dir}/tests/data/specification-latest/",
        "pipeline_dir": f"{tests_dir}/tests/data/pipeline-article-4-direction/"
    }


def test_debug_pipeline_start(env_vars):

    print("")
    print("")
    print("========================================")
    print("Running MapPhase with:")
    print(f" specification_dir: {env_vars['specification_dir']}")
    print(f"      pipeline_dir: {env_vars['pipeline_dir']}")
    print(f"      test_dir_out: {env_vars['test_dir_out']}")
    print(f"     pipeline_name: {env_vars['pipeline_name']}")
    print(f"           dataset: {env_vars['pipeline_name']}")
    print("========================================")

    output_path = Path("./collection")
    if output_path.exists():
        shutil.rmtree(output_path)


def test_map_phase_params_info(env_vars):
    # ctx.obj["PIPELINE"] = Pipeline(pipeline_dir, dataset)
    # ctx.obj["SPECIFICATION"] = Specification(specification_dir)
    # fieldnames = specification.intermediate_fieldnames(pipeline)
    #
    # resource = resource_from_path(input_path)
    # columns = pipeline.columns(resource)
    #
    # column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"  # resource_from_path("")
    endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    pipeline_dir = env_vars['pipeline_dir']
    dataset = env_vars['pipeline_name']
    pipeline = Pipeline(pipeline_dir, dataset)
    specification_dir = env_vars['specification_dir']
    specification = Specification(specification_dir)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    # column_field_log = ColumnFieldLog(dataset=dataset, resource=endpoint)

    column_field_dir = env_vars["test_dir_out"]

    columns = pipeline.columns(resource, endpoint)
    # columns = pipeline.columns(endpoint)
    fieldnames = specification.intermediate_fieldnames(pipeline)
    log = column_field_log

    print("")
    print("")
    print("========================================")
    print("Running MapPhase with:")
    print(f"        fieldnames: {fieldnames}")
    print(f"           columns: {columns}")
    print(f"           log_dir: {column_field_dir}")
    print(f"               log: {log}")
    print("========================================")

    pytest.fieldnames = fieldnames
    pytest.columns = columns
    pytest.log = log


def test_run_pipeline_map_phase(env_vars):
    fieldnames = pytest.fieldnames
    columns = pytest.columns
    column_field_log = pytest.log

    endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    column_field_dir = env_vars["test_dir_out"]

    input_stream = [
        {
            "row": columns,
            "entry-number": 1,
        }
    ]

    phase = MapPhase(
        fieldnames=fieldnames,
        columns=columns,
        log=column_field_log,
    )
    output = [block for block in phase.process(input_stream)]

    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    # column_field_log.save(os.path.join(column_field_dir, endpoint + ".csv"))

    print("")
    print(f">>> DEBUG A: {output}")


# from digital_land.phase.lookup import LookupPhase
#
#
# def test_process_410_redirect():
#     input_stream = [
#         {
#             "row": {
#                 "prefix": "dataset",
#                 "reference": "1",
#                 "organisation": "test",
#             },
#             "entry-number": 1,
#         }
#     ]
#     lookups = {",dataset,1,test": "1"}
#     redirect_lookups = {"1": {"entity": "", "status": "410"}}
#     phase = LookupPhase(
#         entity_field="entity", lookups=lookups, redirect_lookups=redirect_lookups
#     )
#     output = [block for block in phase.process(input_stream)]
#
#     assert output[0]["row"]["entity"] == ""
#
#
# def test_process_301_redirect():
#     input_stream = [
#         {
#             "row": {
#                 "prefix": "dataset",
#                 "reference": "1",
#                 "organisation": "test",
#             },
#             "entry-number": 1,
#         }
#     ]
#     lookups = {",dataset,1,test": "1"}
#     redirect_lookups = {"1": {"entity": "2", "status": "301"}}
#     phase = LookupPhase(
#         entity_field="entity", lookups=lookups, redirect_lookups=redirect_lookups
#     )
#     output = [block for block in phase.process(input_stream)]
#
#     assert output[0]["row"]["entity"] == "2"
#
#
# def test_process_successful_lookup():
#     input_stream = [
#         {
#             "row": {
#                 "prefix": "dataset",
#                 "reference": "1",
#                 "organisation": "test",
#             },
#             "entry-number": 1,
#         }
#     ]
#     lookups = {",dataset,1,test": "1"}
#     phase = LookupPhase(entity_field="entity", lookups=lookups)
#     output = [block for block in phase.process(input_stream)]
#     assert output[0]["row"]["entity"] == "1"
#
