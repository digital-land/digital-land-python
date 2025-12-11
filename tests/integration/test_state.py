import os
import subprocess
import pytest
from datetime import date

from digital_land.state import State

test_hash = "ed4c5979268ad880f7edbdc2047cfcfa6b9ee3b4"
"""
hash of the test files, generated with

$ touch test.txt
$ echo endpoint.csv >> test.txt
& cat tests/data/state/collection_1/endpoint.csv >> test.txt
$ echo old-resource.csv >> test.txt
$ cat tests/data/state/collection_1/old-resource.csv >> test.txt
$ echo resource.csv >> test.txt
$ cat tests/data/state/collection_1/resource.csv >> test.txt
$ echo source.csv >> test.txt
$ cat tests/data/state/collection_1/source.csv >> test.txt
$ sha1sum test.txt
"""


@pytest.fixture
def collection_with_transforms(tmp_path):
    """
    Create a temporary collection directory with known test data
    that will result in a predictable transform count.
    """
    collection_dir = tmp_path / "collection"
    collection_dir.mkdir()

    # Create endpoint.csv with 3 endpoints
    endpoint_file = collection_dir / "endpoint.csv"
    with open(endpoint_file, "w", newline="") as f:
        writer = f
        writer.write(
            "endpoint,endpoint-url,plugin,parameters,entry-date,start-date,end-date\n"
        )
        writer.write("endpoint1,https://example.com/data1,,,2024-01-01,,\n")
        writer.write("endpoint2,https://example.com/data2,,,2024-01-01,,\n")
        writer.write("endpoint3,https://example.com/data3,,,2024-01-01,,\n")

    # Create source.csv linking endpoints to datasets
    source_file = collection_dir / "source.csv"
    with open(source_file, "w", newline="") as f:
        writer = f
        writer.write(
            "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date\n"
        )
        # endpoint1 -> dataset1
        writer.write(
            "source1,,test-collection,,endpoint1,ogl3,org1,dataset1,2024-01-01,,\n"
        )
        # endpoint2 -> dataset1 and dataset2
        writer.write(
            "source2,,test-collection,,endpoint2,ogl3,org2,dataset1;dataset2,2024-01-01,,\n"
        )
        # endpoint3 -> dataset2
        writer.write(
            "source3,,test-collection,,endpoint3,ogl3,org3,dataset2,2024-01-01,,\n"
        )

    # Create resource.csv with 3 resources
    resource_file = collection_dir / "resource.csv"
    with open(resource_file, "w", newline="") as f:
        writer = f
        writer.write(
            "resource,bytes,organisations,datasets,endpoints,start-date,end-date\n"
        )
        # resource1 from endpoint1 (dataset1)
        writer.write("resource1,1000,org1,dataset1,endpoint1,2024-01-01,\n")
        # resource2 from endpoint2 (dataset1 and dataset2)
        writer.write("resource2,2000,org2,dataset1;dataset2,endpoint2,2024-01-01,\n")
        # resource3 from endpoint3 (dataset2)
        writer.write("resource3,3000,org3,dataset2,endpoint3,2024-01-01,\n")

    # Create old-resource.csv (empty but needed)
    old_resource_file = collection_dir / "old-resource.csv"
    with open(old_resource_file, "w", newline="") as f:
        writer = f
        writer.write("old-resource,resource\n")

    # Create log.csv to prevent Collection from trying to load from log items
    log_file = collection_dir / "log.csv"
    with open(log_file, "w", newline="") as f:
        writer = f
        writer.write(
            "bytes,content-type,elapsed,endpoint,entry-date,resource,status,exception,ssl,resource-organisation,endpoint-organisation\n"
        )
        writer.write("1000,text/csv,1.0,endpoint1,2024-01-01,resource1,200,,,org1,\n")
        writer.write("2000,text/csv,1.0,endpoint2,2024-01-01,resource2,200,,,org2,\n")
        writer.write("3000,text/csv,1.0,endpoint3,2024-01-01,resource3,200,,,org3,\n")

    # Expected transform count:
    # dataset1: resource1, resource2 = 2 resources
    # dataset2: resource2, resource3 = 2 resources
    # Total = 4 transformations (resource2 appears in both datasets)

    return collection_dir, 4  # Return directory and expected count


def test_get_code_hash():
    proc = subprocess.run(
        "git log -n 1".split(), cwd=os.path.dirname(__file__), stdout=subprocess.PIPE
    )
    # the first line of this is "commit <hash>"
    hash = proc.stdout.splitlines()[0].split()[1].decode()

    assert hash == State.get_code_hash()


def test_hash_directory():
    assert State.get_dir_hash("tests/data/state/collection") == test_hash


def test_hash_directory_not_exist():
    with pytest.raises(RuntimeError):
        State.get_dir_hash("tests/data/state/non_existant_directory")


def test_hash_directory_with_exclude():
    assert (
        State.get_dir_hash(
            "tests/data/state/collection_exclude",
            ["pipeline.mk", "resource/", "log/", "log.csv"],
        )
        == test_hash
    )


def test_different_dirs_have_different_hashes():
    assert State.get_dir_hash("tests/data/state/collection") != State.get_dir_hash(
        "tests/data/state/collection_blank"
    )


def test_state_build_persist(tmp_path):
    test_dir = "tests/data/state"
    tmp_json = (tmp_path / "state.json").absolute()

    # Generate and save a state
    state_1 = State.build(
        os.path.join(test_dir, "specification"),
        os.path.join(test_dir, "collection"),
        os.path.join(test_dir, "pipeline"),
        os.path.join(test_dir, "resource"),
        True,
    )
    state_1.save(tmp_json)

    # Load it back and check they're the same
    state_2 = State.load(tmp_json)
    assert state_1 == state_2

    # Generate a different one
    state_3 = State.build(
        os.path.join(test_dir, "specification"),
        os.path.join(test_dir, "collection_blank"),
        os.path.join(test_dir, "pipeline"),
        os.path.join(test_dir, "resource"),
        True,
    )

    # Check that's different from the first one
    assert state_2 != state_3


def test_state_build_has_last_updated_date(tmp_path):
    test_dir = "tests/data/state"

    # Generate and save a state
    test_state = State.build(
        os.path.join(test_dir, "specification"),
        os.path.join(test_dir, "collection"),
        os.path.join(test_dir, "pipeline"),
        os.path.join(test_dir, "resource"),
        True,
    )
    assert "last_updated_date" in test_state.keys()
    assert test_state["last_updated_date"] == date.today().isoformat()


def test_get_transform_count(collection_with_transforms):
    collection_dir, expected_count = collection_with_transforms

    # Test the get_transform_count method independently
    count = State.get_transform_count(str(collection_dir))

    # Should return an integer
    assert isinstance(count, int)
    # Should be non-negative
    assert count >= 0
    # Should match expected count
    assert count == expected_count, f"Expected {expected_count} transforms, got {count}"


def test_state_build_has_transform_count(collection_with_transforms, tmp_path):
    collection_dir, expected_count = collection_with_transforms

    # Create minimal directories for other state components
    specification_dir = tmp_path / "specification"
    specification_dir.mkdir()
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    resource_dir = tmp_path / "resource"
    resource_dir.mkdir()

    # Generate a state
    test_state = State.build(
        str(specification_dir),
        str(collection_dir),
        str(pipeline_dir),
        str(resource_dir),
        True,
    )

    # Check that transform_count exists and is an integer
    assert "transform_count" in test_state.keys()
    assert isinstance(test_state["transform_count"], int)
    assert test_state["transform_count"] >= 0
    # Should match expected count
    assert (
        test_state["transform_count"] == expected_count
    ), f"Expected {expected_count} transforms, got {test_state['transform_count']}"
