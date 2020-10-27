import csv
import hashlib
import json
from datetime import datetime

import pytest

from digital_land.collection import Collection

RESOURCES = ["aaa111", "bbb222", "ccc333"]


@pytest.fixture()
def test_collection_dir(tmp_path):
    today = datetime.today().strftime("%Y-%m-%d")
    resource_path = tmp_path / "resource"
    resource_path.mkdir()
    log_path = tmp_path / "log" / today
    log_path.mkdir(parents=True)
    endpoints = []
    sources = []
    for resource in RESOURCES:
        url = f"http://somewhere.org/{resource}"
        endpoint_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        endpoints.append({"endpoint": endpoint_hash, "endpoint-url": url})
        sources.append(
            {"organisation": f"source-{resource}", "endpoint": endpoint_hash}
        )
        log = {
            "resource": resource,
            "url": url,
            "entry-date": today,
        }
        with open(log_path / f"{endpoint_hash}.json", "w") as f:
            json.dump(log, f)
        path = resource_path / resource
        path.touch()
    write_endpoint_csv(tmp_path, endpoints)
    write_source_csv(tmp_path, sources)
    return tmp_path


def write_endpoint_csv(tmp_path, endpoints):
    write_csv(tmp_path / "endpoint.csv", endpoints, ["endpoint", "endpoint-url"])


def write_source_csv(tmp_path, sources):
    write_csv(tmp_path / "source.csv", sources, ["organisation", "endpoint"])


def write_csv(path, items, fieldnames):
    f = open(path, "w", newline="\r\n")
    writer = csv.DictWriter(
        f,
        fieldnames=fieldnames,
    )
    writer.writeheader()
    for item in items:
        writer.writerow(item)
    f.close()


def test_collection(test_collection_dir):
    collection = Collection(test_collection_dir)
    collection.load()
    resources = collection.resources()
    assert resources == RESOURCES


def test_collection_resource_organisation(test_collection_dir):
    collection = Collection(test_collection_dir)
    collection.load()
    assert collection.resource_organisation("aaa111") == ["source-aaa111"]
