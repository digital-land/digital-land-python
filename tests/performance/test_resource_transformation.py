import urllib
from pathlib import Path
import time

import pytest

from digital_land.commands import pipeline_run
from digital_land.specification import Specification
from digital_land.configuration.main import Config
from digital_land.collection import Collection
from digital_land.pipeline import Pipeline
from digital_land.organisation import Organisation

# set the below if you want to use a permenant directory to view data after running test
# o to stopp epeated downloads of tests
DATA_DIRECTORY = "data"


@pytest.mark.parametrize(
    "resource_hash,dataset",
    [
        (
            "1c192f194a6d7cb044006bbe0d7bb7909eed3783eeb8a53026fc15b9fe31a836",
            "article-4-direction-area",
        ),
    ],
)
def test_transformation_performance(
    resource_hash: str, dataset: str, capsys, tmp_path: Path
):
    # endpoint hash TODO we should look at automating the retrieval of this in the future
    # endpoint_hash = 'f4bfb0e3a3f0f0e2e5e1f3c6e4b2a7d8c9e0f1a2b3c4d5e6f7g8h9i0j1k2l3m4'
    if DATA_DIRECTORY:
        data_dir = Path(DATA_DIRECTORY)
    else:
        data_dir = tmp_path / "data"

    data_dir.mkdir(parents=True, exist_ok=True)

    # Specification first as can be used for the others
    specification_dir = data_dir / "specification"
    specification_dir.mkdir(parents=True, exist_ok=True)
    Specification.download(specification_dir)

    spec = Specification(specification_dir)
    collection = spec.dataset[dataset]["collection"]

    if not collection:
        raise ValueError(
            f"Dataset {dataset} does not have a collection defined in the specification"
        )

    # download the configuration files for that collection
    pipeline_dir = data_dir / "pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    Config.download_pipeline_files(path=pipeline_dir, collection=collection)

    # download the resource
    data_collection_url = "https://files.planning.data.gov.uk/"
    resource_url = f"{data_collection_url}{collection}-collection/collection/resource/{resource_hash}"
    resource_path = data_dir / "resource" / f"{resource_hash}"
    resource_path.parent.mkdir(parents=True, exist_ok=True)
    if not resource_path.exists():
        print(f"Downloading {resource_url} to {resource_path}")
        urllib.request.urlretrieve(resource_url, resource_path)
    else:
        print(f"Using existing file {resource_path}")

    # we need to know the endpoint hash for the resource so will need to download the logs
    collection_dir = data_dir / "collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    Collection.download(path=collection_dir, collection=collection)

    # download organisation data
    cache_dir = data_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    org_path = cache_dir / "organisation.csv"
    Organisation.download(path=org_path)

    output_path = data_dir / "transformed" / dataset / f"{resource_hash}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    converted_path = data_dir / "converted" / dataset / f"{resource_hash}.csv"
    converted_path.parent.mkdir(parents=True, exist_ok=True)

    # create pipeline object
    pipeline = Pipeline(pipeline_dir, dataset)

    # create logs
    issue_dir = data_dir / "issues" / dataset
    issue_dir.mkdir(parents=True, exist_ok=True)
    operational_issue_dir = data_dir / "performance" / "operational_issues"
    operational_issue_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir = cache_dir / "column_field" / dataset
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir = cache_dir / "dataset_resource" / dataset
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_resource_dir = cache_dir / "converted_resource" / dataset
    converted_resource_dir.mkdir(parents=True, exist_ok=True)
    output_log_dir = data_dir / "log"
    output_log_dir.mkdir(parents=True, exist_ok=True)

    # get endpoints from the collection TODO include redirects
    collection = Collection(directory=collection_dir)
    collection.load()
    endpoints = collection.resource_endpoints(resource_hash)
    organisations = collection.resource_organisations(resource_hash)
    entry_date = collection.resource_start_date(resource_hash)

    # build config from downloaded files
    config_path = cache_dir / "config.sqlite3"
    config = Config(path=config_path, specification=spec)
    config.create()
    tables = {key: pipeline.path for key in config.tables.keys()}
    config.load(tables)

    # measure length of time
    start_old = time.perf_counter()

    # currently uses pipeline_run as the entrypoint for the old pipeline
    # this may need changing if we alter this function to need new code
    pipeline_run(
        dataset=dataset,
        pipeline=pipeline,
        specification=spec,
        input_path=resource_path,
        output_path=output_path,
        collection_dir=collection_dir,  # TBD: remove, replaced by endpoints, organisations and entry_date
        issue_dir=issue_dir,
        operational_issue_dir=operational_issue_dir,
        organisation_path=org_path,
        save_harmonised=False,
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        converted_resource_dir=converted_resource_dir,
        cache_dir=cache_dir,
        endpoints=endpoints,
        organisations=organisations,
        entry_date=entry_date,
        config_path=config_path,
        resource=resource_hash,
        output_log_dir=output_log_dir,
        converted_path=converted_path,
    )

    end_old = time.perf_counter()

    with capsys.disabled():
        print(f" Original Transform Took: {end_old - start_old:.4f} seconds")

    # now run new version
    print("running new transform")

    start_new = time.perf_counter()

    time.sleep(5)  # replace with new code

    end_new = time.perf_counter()

    with capsys.disabled():
        print(f" New Transform Took: {end_new - start_new:.4f} seconds")
