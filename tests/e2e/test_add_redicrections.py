import os
import csv
import pytest
from pathlib import Path
from digital_land.commands import add_redirections


@pytest.fixture
def csv_file_path(tmp_path):
    """
    Populates the input csv file with required data and returns the file path
    """
    entities_csv_path = os.path.join(tmp_path, "redirections.csv")

    row = {
        "entity_LPA": "100",
        "entity_HE": "200",
    }
    fieldnames = row.keys()
    with open(entities_csv_path, "w") as f:

        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return entities_csv_path


def test_add_redirections_success(csv_file_path, tmp_path):

    add_redirections(csv_file_path=csv_file_path, pipeline_dir=tmp_path)
    old_entity_path = Path(tmp_path) / "old-entity.csv"

    with open(old_entity_path, "r") as mock_file:
        reader = csv.DictReader(mock_file)
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["old-entity"] == "200"
        assert rows[0]["status"] == "301"
        assert rows[0]["entity"] == "100"
