import pytest

from digital_land.utils.hash import hash_dir

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
def test_dir(tmp_path):
    """Creates a test directory with known files and contents. fo use across multiplle tests"""
    # build and write files in a directoy
    file_strings = {
        "file1.txt": "This is file one.",
        "file2.txt": "This is file two.",
        "file3.txt": "This is file three.",
    }

    dir = tmp_path / "test_dir"

    for filename, content in file_strings.items():
        file_path = dir / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)

    return dir


def test_hash_dir_hashes_currently(test_dir):
    # now run hash_dir on this directory
    computed_hash = hash_dir(test_dir)
    assert computed_hash == "9322b8a8dec1dafa52306442a8233955bce49dd8"


def test_hash_directory_not_exist(tmp_path):
    with pytest.raises(RuntimeError):
        hash_dir(tmp_path / "non_existent_directory")


def test_hash_directory_not_a_directory_but_a_file(tmp_path):
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir(parents=True, exist_ok=True)
    test_file = test_dir / "file.txt"
    with open(test_file, "w") as f:
        f.write("This is a test file.")

    with pytest.raises(RuntimeError):
        hash_dir(test_file)


def test_hash_directory_with_exclude(test_dir):
    computed_hash = hash_dir(test_dir, exclude=["file2.txt"])
    assert computed_hash == "682c69058b7099b9404e6c9480acbbd1b56f244d"
