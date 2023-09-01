import pytest
import os
import csv

from digital_land.pipeline import Lookups


@pytest.fixture
def pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    # create lookups
    row = {
        "prefix": "ancient-woodland",
        "resource": "",
        "organisation": "local-authority-eng:ABC",
        "reference": "ABC_0001",
        "entity": "1234567",
    }
    fieldnames = row.keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return pipeline_dir


@pytest.fixture
def empty_pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    return pipeline_dir


def test_lookups_load_csv_success(
    pipeline_dir,
):
    """
    test csv loading functionality
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    expected_num_entries = 1
    assert len(lookups.entries) == expected_num_entries


def test_lookups_load_csv_failure(
    empty_pipeline_dir,
):
    """
    test csv loading functionality when file absent
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(empty_pipeline_dir)

    with pytest.raises(FileNotFoundError) as fnf_err:
        lookups.load_csv()

    expected_exception = "No such file or directory"
    assert expected_exception in str(fnf_err.value)


def test_lookups_get_max_entity_success(
    pipeline_dir,
):
    """
    test entity num generation functionality
    :param pipeline_dir:
    :return:
    """
    pipeline_name = "ancient-woodland"

    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    expected_next_entity_num = 1
    assert lookups.entity_num_gen.next() is expected_next_entity_num

    max_entity_num = lookups.get_max_entity(pipeline_name)
    lookups.entity_num_gen.state["current"] = max_entity_num
    lookups.entity_num_gen.state["range_max"] = max_entity_num + 10

    expected_next_entity_num = 1234568
    assert lookups.entity_num_gen.next() == expected_next_entity_num


def test_lookups_validate_entry_success(
    pipeline_dir,
):
    """
    test csv validate_entry functionality
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    entry = {
        "prefix": "ancient-woodland",
        "resource": "",
        "organisation": "government-organisation:D1342",
        "reference": "1",
        "entity": "",
    }

    lookups.validate_entry(entry)


def test_lookups_validate_entry_failure(
    pipeline_dir,
):
    """
    test csv validate_entry functionality for various errors
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    entry = {}
    with pytest.raises(ValueError):
        lookups.validate_entry(entry)

    entry = {
        "prefix": "",
    }
    with pytest.raises(ValueError):
        lookups.validate_entry(entry)

    entry = {
        "prefix": "",
        "organisation": "",
    }
    with pytest.raises(ValueError):
        lookups.validate_entry(entry)

    entry = {
        "prefix": "",
        "organisation": "",
        "reference": "",
    }
    with pytest.raises(ValueError):
        lookups.validate_entry(entry)

    entry = {
        "prefix": "",
        "resource": "",
        "organisation": "",
        "reference": "",
        "entity": "",
    }
    with pytest.raises(ValueError):
        lookups.validate_entry(entry)


def test_lookups_add_entry_success(
    pipeline_dir,
):
    """
    test csv validate_entry functionality
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    entry = {
        "prefix": "ancient-woodland",
        "resource": "",
        "organisation": "government-organisation:D1342",
        "reference": "1",
        "entity": "",
    }

    lookups.add_entry(entry)


def test_lookups_add_entry_failure(
    pipeline_dir,
):
    """
    test csv add_entry functionality for validation errors
    :param pipeline_dir:
    :return:
    """
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    entry = {}
    with pytest.raises(ValueError):
        lookups.add_entry(entry)

    entry = {
        "prefix": "",
    }
    with pytest.raises(ValueError):
        lookups.add_entry(entry)

    entry = {
        "prefix": "",
        "organisation": "",
    }
    with pytest.raises(ValueError):
        lookups.add_entry(entry)

    entry = {
        "prefix": "",
        "organisation": "",
        "reference": "",
    }
    with pytest.raises(ValueError):
        lookups.add_entry(entry)

    entry = {
        "prefix": "",
        "resource": "",
        "organisation": "",
        "reference": "",
        "entity": "",
    }
    with pytest.raises(ValueError):
        lookups.add_entry(entry)
