import json
from datetime import datetime

import pytest

from digital_land.collection import Collection

RESOURCES = ["aaa111", "bbb222", "ccc333"]


@pytest.fixture()
def test_collection_dir(tmp_path):
    today = datetime.today().strftime("%Y-%m-%d")
    log_path = tmp_path / "log" / today
    log_path.mkdir(parents=True)
    resource_path = tmp_path / "resource"
    resource_path.mkdir()
    for resource in RESOURCES:
        log = {
            "resource": resource,
            "url": f"http://somewhere.org/{resource}",
            "entry-date": today,
        }
        with open(log_path / f"{resource}.json", "w") as f:
            json.dump(log, f)
        path = resource_path / resource
        path.touch()
    return tmp_path


def test_collection(test_collection_dir):
    collection = Collection(
        log_dir=test_collection_dir / "log",
        resource_dir=test_collection_dir / "resource",
    )
    collection.load()
    resources = collection.resources()
    assert resources == RESOURCES
