import csv
import json
import urllib.request
import pytest

import os.path
from os import path

from datetime import datetime
from click.testing import CliRunner
from digital_land.cli import cli

from digital_land.collect import Collector

from tests.utils.helpers import hash_digest

ENDPOINT = "https://raw.githubusercontent.com/digital-land/digital-land-python/main/tests/data/resource_examples/csv.csv"
COLLECTION_DIR = "./collection"


@pytest.fixture()
def endpoint_csv(tmp_path):
    p = tmp_path / "endpoint.csv"
    fieldnames = ["endpoint", "endpoint-url"]
    with open(p, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"endpoint": hash_digest(ENDPOINT), "endpoint-url": ENDPOINT})
    return str(p)


@pytest.fixture()
def collection_dir(tmp_path):
    c = tmp_path / "collection"
    c.mkdir()
    return str(c)


def test_collect(endpoint_csv, collection_dir):
    args = [
        "collect",
        "--collection-dir",
        collection_dir,
        endpoint_csv,
    ]

    runner = CliRunner()
    result = runner.invoke(cli, args)

    log_date = datetime.utcnow().isoformat()[:10]
    log_file = f"{collection_dir}/log/{log_date}/{hash_digest(ENDPOINT)}.json"

    assert 0 == result.exit_code

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

def test_fetch_overwrite_endpoint_logs(url, pipeline):
    """fetch a single source endpoint URL, and add it to the collection"""
    collector = Collector(pipeline.name)
    collector.fetch(url)       

    assert os.path exists("*.log") == True