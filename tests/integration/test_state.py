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
