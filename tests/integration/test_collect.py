import shutil
import csv
import urllib.request
import json
import hashlib
from datetime import datetime
from helpers import execute
from pathlib import Path

import pytest

ENDPOINT = "https://www.registers.service.gov.uk/registers/country/download-csv"
COLLECTION_DIR = "./collection"


@pytest.fixture()
def endpoint_csv(tmp_path):
    p = tmp_path / "endpoint.csv"
    fieldnames = ["endpoint", "endpoint-url"]
    with open(p, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"endpoint": hash_digest(ENDPOINT), "endpoint-url": ENDPOINT})
    return p


def hash_digest(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def test_collect(endpoint_csv):
    returncode, outs, errs = execute(
        [
            "digital-land",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "collect",
            endpoint_csv,
        ]
    )

    log_date = datetime.utcnow().isoformat()[:10]
    log_file = f"{COLLECTION_DIR}/log/{log_date}/{hash_digest(ENDPOINT)}.json"

    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs

    resource = read_log(log_file)
    assert resource
    assert resource_collected(resource)


def teardown_function():
    if Path(COLLECTION_DIR).is_dir():
        shutil.rmtree(Path(COLLECTION_DIR))


def read_log(log_file):
    data = open(log_file).read()
    log = json.loads(data)
    assert log["endpoint-url"] == ENDPOINT
    return log["resource"]


def resource_collected(resource):
    saved = open(f"{COLLECTION_DIR}/resource/{resource}").read().rstrip()
    raw = urllib.request.urlopen(ENDPOINT).read().decode("utf-8")
    downloaded = "\n".join(raw.splitlines())  # Convert CRLF to LF
    return saved == downloaded
