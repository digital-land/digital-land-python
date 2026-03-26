import pytest
from digital_land.utils.hash_utils import hash_directory


def test_hash_directory_returns_hex_string(tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    result = hash_directory(str(tmp_path))
    assert isinstance(result, str)
    assert len(result) == 40  # SHA1 produces a 40-character hex digest


def test_hash_directory_is_stable(tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    assert hash_directory(str(tmp_path)) == hash_directory(str(tmp_path))


def test_hash_directory_changes_on_content_change(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("hello")
    hash_before = hash_directory(str(tmp_path))
    f.write_text("world")
    assert hash_directory(str(tmp_path)) != hash_before


def test_hash_directory_changes_on_new_file(tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    hash_before = hash_directory(str(tmp_path))
    (tmp_path / "new_file.txt").write_text("new")
    assert hash_directory(str(tmp_path)) != hash_before


def test_hash_directory_changes_on_rename(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("hello")
    hash_before = hash_directory(str(tmp_path))
    f.rename(tmp_path / "renamed.txt")
    assert hash_directory(str(tmp_path)) != hash_before


def test_hash_directory_raises_for_nonexistent_dir():
    with pytest.raises(RuntimeError):
        hash_directory("/nonexistent/path/that/does/not/exist")


def test_hash_directory_exclude_omits_matching_files(tmp_path):
    (tmp_path / "include.txt").write_text("included")
    (tmp_path / "exclude.txt").write_text("excluded")
    hash_all = hash_directory(str(tmp_path))
    hash_excluded = hash_directory(str(tmp_path), exclude=["exclude"])
    assert hash_all != hash_excluded


def test_hash_directory_exclude_is_stable_when_excluded_file_changes(tmp_path):
    (tmp_path / "include.txt").write_text("included")
    excluded = tmp_path / "exclude.txt"
    excluded.write_text("original")
    hash_before = hash_directory(str(tmp_path), exclude=["exclude"])
    excluded.write_text("changed")
    assert hash_directory(str(tmp_path), exclude=["exclude"]) == hash_before
