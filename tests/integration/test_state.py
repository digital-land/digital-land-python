import os
import subprocess

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
    hash = proc.stdout.splitlines()[0].split()[1].decode()

    state = State()
    assert hash == state.get_code_hash()


def test_hash_directory():
    state = State()
    assert state.get_dir_hash("tests/data/state/collection_1") == test_hash


def test_hash_directory_with_exclude():
    state = State()
    assert (
        state.get_dir_hash(
            "tests/data/state/collection_2", ["resource/", "log/", "log.csv"]
        )
        == test_hash
    )
