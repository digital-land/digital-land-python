import csv
import urllib.request
import json
from datetime import datetime
from tests.utils.helpers import execute, hash_digest

import pytest

ENDPOINT = "https://raw.githubusercontent.com/digital-land/digital-land-python/main/tests/data/resource_examples/csv.csv"


@pytest.fixture()
def endpoint_csv(tmp_path):
    p = tmp_path / "endpoint.csv"
    fieldnames = ["endpoint", "endpoint-url"]
    with open(p, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"endpoint": hash_digest(ENDPOINT), "endpoint-url": ENDPOINT})
    return p


@pytest.fixture()
def collection_dir(tmp_path):
    c = tmp_path / "collection"
    c.mkdir()
    return c


def test_collect(endpoint_csv, collection_dir):
    collection_dir = collection_dir
    returncode, outs, errs = execute(
        [
            "digital-land",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "collect",
            "-c",
            collection_dir,
            endpoint_csv,
        ]
    )

    log_date = datetime.utcnow().isoformat()[:10]
    log_file = f"{collection_dir}/log/{log_date}/{hash_digest(ENDPOINT)}.json"

    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs

    resource = read_log(log_file)
    assert resource
    assert resource_collected(collection_dir, resource)


def read_log(log_file):
    data = open(log_file).read()
    log = json.loads(data)
    assert log["endpoint-url"] == ENDPOINT
    return log["resource"]


def resource_collected(collection_dir, resource):
    saved = open(f"{collection_dir}/resource/{resource}").read().rstrip()
    raw = urllib.request.urlopen(ENDPOINT).read().decode("utf-8")
    downloaded = "\n".join(raw.splitlines())  # Convert CRLF to LF
    return saved == downloaded
