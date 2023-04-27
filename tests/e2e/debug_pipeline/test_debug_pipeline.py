import os
from pathlib import Path
import shutil

import pytest

from tests.e2e.debug_pipeline.funcs.test_cli_debug_funcs import \
    get_endpoints_info, get_endpoints, download_endpoints, \
    endpoint_collection, run_collection_pipeline

def pytest_namespace():
    return {
        "endpoint_hashes": [],
        "log_entries": None,
        "resource_entries": None
    }

# This test is a utility function
# It expands the debug_pipeline function of commands.py,
# reporting on each step, and providing useful info
# to the user.
@pytest.fixture
def env_vars_default(request):

    root_dir = request.config.rootdir
    tests_dir = Path(root_dir).resolve().parent.parent

    return {
        "organisation": "",
        "pipeline_name": "",
        "dataset_name": "",
        "endpoint_path": f"{tests_dir}/tests/data/collection/",
        "collection_dir": f"{tests_dir}/tests/data/collection/",
        "collection_dir_out": f"{tests_dir}/tests/e2e/debug_pipeline/collection/",
        "specification_dir": f"{tests_dir}/tests/data/specification/"
    }


@pytest.fixture
def env_vars_national_park(request):

    root_dir = request.config.rootdir
    tests_dir = Path(root_dir).resolve()

    return {
        "organisation": "government-organisation:D303",
        "pipeline_name": "national-park",
        "dataset_name": "national-park",
        "endpoint_path": f"{tests_dir}/tests/data/collection-national-park/",
        "collection_dir": f"{tests_dir}/tests/data/collection-national-park/",
        "collection_dir_out": f"{tests_dir}/tests/e2e/debug_pipeline/collection/",
        "specification_dir": f"{tests_dir}/tests/data/specification/"
    }


@pytest.fixture
def env_vars_article_4_direction(request):

    root_dir = request.config.rootdir
    tests_dir = Path(root_dir).resolve()

    return {
        "organisation": "local-authority-eng:SWK",
        "pipeline_name": "article-4-direction-area",
        "dataset_name": "article-4-direction",
        "endpoint_path": f"{tests_dir}/tests/data/collection-article-4-direction/",
        "collection_dir": f"{tests_dir}/tests/data/collection-article-4-direction/",
        "collection_dir_out": f"{root_dir}/tests/e2e/debug_pipeline/collection/",
        "specification_dir": f"{tests_dir}/tests/data/specification/"
    }


@pytest.fixture
def env_vars(env_vars_default, env_vars_article_4_direction):
    return env_vars_article_4_direction


def test_debug_pipeline_start(env_vars):
    print("")
    print("")
    print("========================================")
    print("Running debug_pipeline with:")
    print(f"      organisation: {env_vars['organisation']}")
    print(f"     pipeline_name: {env_vars['pipeline_name']}")
    print(f"      dataset_name: {env_vars['dataset_name']}")
    print(f"     endpoint_path: {env_vars['endpoint_path']}")
    print(f"    collection_dir: {env_vars['collection_dir']}")
    print(f"collection_dir_out: {env_vars['collection_dir_out']}")
    print(f" specification_dir: {env_vars['specification_dir']}")
    print("========================================")

    output_path = Path("./collection")
    if output_path.exists():
        shutil.rmtree(output_path)


def test_get_endpoints_info(env_vars):
    collection_dir = env_vars['collection_dir']

    sut_result = get_endpoints_info(collection_dir)

    assert sut_result is True


def test_get_endpoints(env_vars):
    organisation = env_vars['organisation']
    pipeline_name = env_vars['pipeline_name']
    collection_dir = env_vars['collection_dir']

    endpoint_hashes = get_endpoints(organisation, pipeline_name, collection_dir)
    pytest.endpoint_hashes = endpoint_hashes

    assert endpoint_hashes is not None
    assert endpoint_hashes != []


def test_download_endpoints(env_vars):
    pipeline_name = env_vars['pipeline_name']
    endpoint_path = env_vars['endpoint_path']
    endpoint_hashes = pytest.endpoint_hashes

    sut_result = download_endpoints(pipeline_name, endpoint_path, endpoint_hashes)

    assert sut_result is True


def move_collection_files_to_test_collection_dir(source_csv_path, endpoint_csv_path, collection_dir):

    if os.path.isfile(source_csv_path):
        shutil.copy(source_csv_path, collection_dir)

    if os.path.isfile(endpoint_csv_path):
        shutil.copy(endpoint_csv_path, collection_dir)


def test_endpoint_collection(env_vars):
    source_csv_path = f"{env_vars['endpoint_path']}source.csv"
    endpoint_csv_path = f"{env_vars['endpoint_path']}endpoint.csv"
    collection_dir_out = env_vars['collection_dir_out']
    endpoint_hashes = pytest.endpoint_hashes

    # Move files from source collection dir to test dir,
    # to keep original collection clean of test-run artifacts
    move_collection_files_to_test_collection_dir(source_csv_path, endpoint_csv_path, collection_dir_out)

    (log_entries, resource_entries) = endpoint_collection(collection_dir_out, endpoint_hashes)

    pytest.log_entries = log_entries
    pytest.resource_entries = resource_entries

    assert log_entries is not None
    assert resource_entries is not None


def test_run_collection_pipeline(env_vars):
    pipeline_name = env_vars['pipeline_name']
    collection_dir_out = env_vars['collection_dir_out']
    dataset_name = env_vars['dataset_name']
    endpoint_hashes = pytest.endpoint_hashes
    log_entries = pytest.log_entries
    resource_entries = pytest.resource_entries

    sut_result = run_collection_pipeline(collection_dir_out, pipeline_name, dataset_name, resource_entries, log_entries)


# use article-4-direction-collection
# specification = Specification(specification_dir)
