import os
import pathlib
import shutil

import pytest

from tests.e2e.debug_pipline.funcs.test_cli_debug_funcs \
    import get_endpoints_info, get_endpoints, download_endpoints


def pytest_namespace():
    return {"endpoint_hashes": []}


# This test is a utility function
# It expands the debug_pipeline function of commands.py,
# reporting on each step, and providing useful info
# to the user.
@pytest.fixture
def env_vars_default():
    return {
        "organisation": "",
        "pipeline": "",
        "endpoint_path": "../data/collection/",
        "collection_dir": "../data/collection/"
    }


@pytest.fixture
def env_vars_national_park():
    return {
        "organisation": "government-organisation:D303",
        "pipeline": "national-park",
        "endpoint_path": "../../data/national-park-collection/",
        "collection_dir": "../../data/national-park-collection/"
    }


@pytest.fixture
def env_vars(env_vars_default, env_vars_national_park):
    return env_vars_national_park


def test_debug_pipeline_start(env_vars):
    print("")
    print("")
    print("========================================")
    print("Running debug_pipeline with:")
    print(f"  organisation: {env_vars['organisation']}")
    print(f"      pipeline: {env_vars['pipeline']}")
    print(f" endpoint_path: {env_vars['endpoint_path']}")
    print(f"collection_dir: {env_vars['collection_dir']}")
    print("========================================")

    output_path = pathlib.Path("./collection")
    if output_path.exists():
        shutil.rmtree(output_path)


def test_get_endpoints_info(env_vars):
    collection_dir = env_vars['collection_dir']

    sut_result = get_endpoints_info(collection_dir)

    assert sut_result is True


def test_get_endpoints(env_vars):
    organisation = env_vars['organisation']
    pipeline = env_vars['pipeline']
    collection_dir = env_vars['collection_dir']

    endpoint_hashes = get_endpoints(organisation, pipeline, collection_dir)
    pytest.endpoint_hashes = endpoint_hashes

    assert endpoint_hashes is not None
    assert endpoint_hashes != []


def test_download_endpoints(env_vars):
    pipeline = env_vars['pipeline']
    endpoint_path = env_vars['endpoint_path']
    endpoint_hashes = pytest.endpoint_hashes

    sut_result = download_endpoints(pipeline, endpoint_path, endpoint_hashes)

    assert sut_result is True

