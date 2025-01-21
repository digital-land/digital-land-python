import numpy as np
import pytest
from digital_land.collect import Collector
from digital_land.collection import Collection
from pathlib import Path

import hashlib
import time


# Hasher function for file to check if things have changed
def file_hash(file_path):
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# line count function
def line_count(path):
    with open(path) as file:
        return sum(1 for _ in file)


# Fixture to create a shared temporary directory
@pytest.fixture(scope="session")
def temp_dir(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("shared_session_temp_dir")
    yield temp_dir


# Create collection directory from temp_dir
@pytest.fixture(scope="session")
def collection_dir(temp_dir):
    temp_dir_path = Path(temp_dir)
    collection_dir = temp_dir_path / "collection"
    collection_dir.mkdir(exist_ok=True)
    return collection_dir


@pytest.fixture
def test_dataset_collector(temp_dir, collection_dir):
    # Set up temporary files with mock data
    endpoint_path = [
        collection_dir / "endpoint.csv",
    ]
    columns = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    # Test data for the tables. This has been copied from conservation-area
    data = [
        [
            "833e2e15068ee0aa57d94513b1ec5035b681b22a626bb080749aa323391d2b50",
            "https://maps.trafford.gov.uk/getows.ashx?mapsource=Trafford/inspire&SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=conservation_areas",
            "",
            "wfs",
            "2020-09-06T11:53:32Z",
            "2020-10-01",
            "",
        ],
        [  # This has an end date so won't be downloaded
            "1e54db15e6aacb60345575ec20d558851d722a4711bb20b324ccb7cbffd1afa9",
            "https://www.sedgemoor.gov.uk/media/5934/Conservation-Areas/zip/Conservation_Areas_May_2020.zip",
            "",
            "",
            "2020-09-11T17:01:47Z",
            "2020-10-01",
            "2024-09-05",  # Check end date
        ],
        [
            "7cb1692a54e5a767247f2539cc65d2f02c0d0e47f81aef5d67db0363709a99b5",
            "http://mapping.dudley.gov.uk/getows.ashx?mapsource=DMBC/inspire&service=WFS&version=1.1.0&Request=GetFeature&TypeName=conservation_areas",
            "",
            "wfs",
            "2020-09-06T20:57:57Z",
            "2020-10-01",
            "",
        ],
    ]

    with open(endpoint_path[0], "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(
                ",".join(map(lambda x: str(x) if x is not np.nan else "", row)) + "\n"
            )

    source_path = [
        collection_dir / "source.csv",
    ]
    columns = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]
    data = [
        [
            "ea9fda79fc39b986b0fc73e3df34a0e7",
            "",
            "conservation-area",
            "https://data.gov.uk/dataset/d2284a6c-82bd-4b7c-9dc8-cecbe2fe0969/conservation-areas,833e2e15068ee0aa57d94513b1ec5035b681b22a626bb080749aa323391d2b50",
            "",
            "local-authority:TRF",
            "conservation-area",
            "2020-11-29T12:18:40Z",
            "2020-11-29",
            "",
        ],
        [
            "61e8ea892a9df714df55595f35d31163",
            "",
            "conservation-area",
            "https://data.gov.uk/dataset/5b6c6254-1493-4840-a507-21f7b711582e/sedgemoor-conservation-areas,1e54db15e6aacb60345575ec20d558851d722a4711bb20b324ccb7cbffd1afa9",
            "",
            "local-authority:SEG",
            "conservation-area",
            "2020-11-29T12:18:40Z",
            "2020-11-29",
            "2024-09-05",
        ],
        [
            "c82cea2d0cad4dec14133f30dfd41b92",
            "",
            "conservation-area",
            "https://data.gov.uk/dataset/34998a72-3d6e-40eb-a5bd-f9bc9c4ec1e3/conservation-areas",
            "7cb1692a54e5a767247f2539cc65d2f02c0d0e47f81aef5d67db0363709a99b5",
            "",
            "local-authority:DUD",
            "conservation-area",
            "2020-11-29T12:18:40Z",
            "2020-11-29",
            "",
        ],
    ]
    with open(source_path[0], "w") as f:
        f.write(",".join(columns) + "\n")
        for row in data:
            f.write(
                ",".join(map(lambda x: str(x) if x is not np.nan else "", row)) + "\n"
            )

    # Create blank log and resource.csv files (they will be filled in later)
    columns = [
        "bytes",
        "content-type",
        "elapsed",
        "endpoint",
        "resource",
        "status",
        "entry-date",
        "start-date",
        "end-date",
        "exception",
    ]
    with open(collection_dir / "log.csv", "w") as f:
        f.write(",".join(columns) + "\n")
    columns = [
        "resource",
        "bytes",
        "organisations",
        "datasets",
        "endpoints",
        "start-date",
        "end-date",
    ]
    with open(collection_dir / "resource.csv", "w") as f:
        f.write(",".join(columns) + "\n")

    # Instantiate the Collector class
    collector = Collector(collection_dir=collection_dir)

    yield collector


def test_collector(test_dataset_collector, collection_dir):
    # Test 0: Has data been set up correctly (Collector class)
    # test that we have an endpoint.csv file available
    endpoint_path = collection_dir / "endpoint.csv"
    assert Path.exists(endpoint_path), "endpoint_path does not exist"

    # test that we collect the json files (replicating 'make collect'
    test_dataset_collector.collect(endpoint_path=endpoint_path)
    json_files = list(collection_dir.glob("**/*.json"))
    assert len(json_files) == 2, "Not all json files downloaded"

    # test that the log and resource.csv files only have headers in them
    assert line_count(collection_dir / "log.csv") <= 1, "Info in the log.csv file"
    assert (
        line_count(collection_dir / "resource.csv") <= 1
    ), "Info in the resource.csv file"
    # Save details fo original log and resource files to use with comparisons later
    log_modification_time0 = (collection_dir / "log.csv").stat().st_mtime
    resource_modification_time0 = (collection_dir / "resource.csv").stat().st_mtime
    log_hash0 = file_hash(collection_dir / "log.csv")
    resource_hash0 = file_hash(collection_dir / "resource.csv")

    # Test 1: Does the Collection object work as expected
    collection_object1 = Collection(name=None, directory=collection_dir)
    collection_object1.load()
    collection_object1.update()
    collection_object1.save_csv()
    # Check that info has been saved to log and resource.csv
    assert (
        line_count(collection_dir / "log.csv") > 1
    ), "Info not placed in the log.csv file after first run"
    assert (
        line_count(collection_dir / "resource.csv") > 1
    ), "Info not placed in the resource.csv file after first run"

    # Save details fo original log and resource files to use with comparisons later
    log_modification_time1 = (collection_dir / "log.csv").stat().st_mtime
    resource_modification_time1 = (collection_dir / "resource.csv").stat().st_mtime
    log_hash1 = file_hash(collection_dir / "log.csv")
    resource_hash1 = file_hash(collection_dir / "resource.csv")

    # assert that the files are updated
    # note that here we want the files to be different but for later tests we want them to be unchanged
    assert (
        log_modification_time0 != log_modification_time1
    ), "log file not updated after first run"
    assert (
        resource_modification_time0 != resource_modification_time1
    ), "resource file not updated after first run"
    assert log_hash0 != log_hash1, "log file not updated after first run"
    assert resource_hash0 != resource_hash1, "resource file not updated after first run"

    # Always add a sleep before we start a new test since we want to check the timings of when files were created
    # and we make get a situation where the file information has not finished updating before we're asking for the
    # creation time. A small sleep will give the system time to update and make sure it doesn't fail the CI tests
    time.sleep(2)
    # Test 2: check that the log and resource.csv files are recreated if they are deleted
    Path.unlink(collection_dir / "log.csv")
    Path.unlink(collection_dir / "resource.csv")
    assert not Path.exists(collection_dir / "log.csv"), "log.csv file not deleted"
    assert not Path.exists(
        collection_dir / "resource.csv"
    ), "resource.csv file not deleted"

    # Need to create a new Collection object, otherwise info from original Collection class gets saved and we're
    # adding repeated data
    collection_object2 = Collection(name=None, directory=collection_dir)
    collection_object2.load()
    collection_object2.update()
    collection_object2.save_csv()

    assert Path.exists(
        collection_dir / "log.csv"
    ), "log.csv file not created after second run"
    assert Path.exists(
        collection_dir / "resource.csv"
    ), "resource.csv file not created after second run"

    # In this test we want the modification time to be different but the file hashes to be the same (to represent the
    # fact that the files have been 'updated' but they have exactly the same values as before)
    log_modification_time2 = Path(collection_dir / "log.csv").stat().st_mtime
    resource_modification_time2 = Path(collection_dir / "resource.csv").stat().st_mtime
    log_hash2 = file_hash(Path(collection_dir / "log.csv"))
    resource_hash2 = file_hash(Path(collection_dir / "resource.csv"))

    assert (
        log_modification_time2 != log_modification_time1
    ), "log file not updated after second run"
    assert (
        resource_modification_time2 != resource_modification_time1
    ), "resource file not updated after second run"
    assert log_hash2 == log_hash1, "log file changed after second run"
    assert resource_hash2 == resource_hash1, "resource file after second run"

    # Test 3: check that the log and resource.csv files are not changed if we rerun `make collection`
    time.sleep(2)
    collection_object3 = Collection(name=None, directory=collection_dir)
    collection_object3.load()
    collection_object3.update()
    collection_object3.save_csv()

    assert Path.exists(
        collection_dir / "log.csv"
    ), "log.csv file not created after third run"
    assert Path.exists(
        collection_dir / "resource.csv"
    ), "resource.csv file not created after third run"

    log_modification_time3 = Path(collection_dir / "log.csv").stat().st_mtime
    resource_modification_time3 = Path(collection_dir / "resource.csv").stat().st_mtime
    log_hash3 = file_hash(Path(collection_dir / "log.csv"))
    resource_hash3 = file_hash(Path(collection_dir / "resource.csv"))

    assert (
        log_modification_time3 != log_modification_time2
    ), "log file not updated after third run"
    assert (
        resource_modification_time3 != resource_modification_time2
    ), "resource file not updated after third run"
    assert log_hash3 == log_hash1, "log file changed after third run"
    assert resource_hash3 == resource_hash1, "resource file after third run"

    # Test 4: check that if a json file is deleted then it is collected
    time.sleep(2)
    json_files = np.sort(json_files)
    Path.unlink(json_files[0])
    json_files = list(collection_dir.glob("**/*.json"))
    assert len(json_files) == 1, "json file failed to delete"
    test_dataset_collector.collect(endpoint_path=endpoint_path)
    json_files = list(collection_dir.glob("**/*.json"))
    assert len(json_files) == 2, "json file not downloaded after initial deletion"

    collection_object4 = Collection(name=None, directory=collection_dir)
    collection_object4.load()
    collection_object4.update()
    collection_object4.save_csv()

    assert Path.exists(
        collection_dir / "log.csv"
    ), "log.csv file not created after fourth run"
    assert Path.exists(
        collection_dir / "resource.csv"
    ), "resource.csv file not created after fourth run"

    log_modification_time4 = Path(collection_dir / "log.csv").stat().st_mtime
    resource_modification_time4 = Path(collection_dir / "resource.csv").stat().st_mtime
    log_hash4 = file_hash(Path(collection_dir / "log.csv"))
    resource_hash4 = file_hash(Path(collection_dir / "resource.csv"))

    assert (
        log_modification_time4 != log_modification_time3
    ), "log file not updated  after fourth run"
    assert (
        resource_modification_time4 != resource_modification_time3
    ), "resource file not updated  after fourth run"
    assert log_hash4 == log_hash1, "log file changed after first run"
    assert resource_hash4 == resource_hash1, "resource file after first run"

    # Test 5: check that if an extra json file is added then it does not alter the log and resource files
    time.sleep(2)
    Path.touch(collection_dir / "aaaaaaaaaaa.json")

    json_files = list(collection_dir.glob("**/*.json"))
    assert len(json_files) == 3, "manual json file not saved"

    collection_object5 = Collection(name=None, directory=collection_dir)
    collection_object5.load()
    collection_object5.update()
    collection_object5.save_csv()

    assert Path.exists(
        collection_dir / "log.csv"
    ), "log.csv file does not exist after fifth run"
    assert Path.exists(
        collection_dir / "resource.csv"
    ), "resource.csv file does not exist after fifth run"

    log_modification_time5 = Path(collection_dir / "log.csv").stat().st_mtime
    resource_modification_time5 = Path(collection_dir / "resource.csv").stat().st_mtime
    log_hash5 = file_hash(Path(collection_dir / "log.csv"))
    resource_hash5 = file_hash(Path(collection_dir / "resource.csv"))

    assert (
        log_modification_time5 != log_modification_time4
    ), "log file not updated after fifth run"
    assert (
        resource_modification_time5 != resource_modification_time4
    ), "resource file not updated after fifth run"
    assert log_hash5 == log_hash1, "log file changed after fifth run"
    assert resource_hash5 == resource_hash1, "resource file after fifth run"
