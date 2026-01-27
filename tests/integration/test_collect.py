import csv
import json
import urllib.request
import pytest

from pathlib import Path

from datetime import datetime
from click.testing import CliRunner
from digital_land.collect import FetchStatus
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


def create_mock_response(mocker, status_code=200, content=b"", content_type="text/csv"):
    """Helper to create a mock requests response object."""
    mock_response = mocker.Mock()
    mock_response.status_code = status_code
    mock_response.content = content
    mock_response.headers = {"Content-Type": content_type}
    mock_response.request = mocker.Mock()
    mock_response.request.headers = {"User-Agent": "test-agent"}
    return mock_response


class TestCollector:
    def test_fetch_overwrite_endpoint_logs(self, collection_dir, tmp_path, mocker):
        """fetch a single source endpoint URL, and add it to the collection"""
        # -- Arrange --
        url = "https://example.com/test-endpoint.csv"
        mock_content = b"reference,name\n1,Test Name\n2,Another Name"

        mock_response = create_mock_response(
            mocker, status_code=500, content=mock_content
        )

        mocker.patch(
            "digital_land.collect.requests.Session.get",
            return_value=mock_response,
        )

        log_dir = Path(collection_dir) / "log"
        resource_dir = Path(collection_dir) / "resource"
        collector = Collector(resource_dir=str(resource_dir), log_dir=str(log_dir))
        fetch_status, log = collector.fetch(url=url, refill_todays_logs=True)

        # -- Act --
        # run initial fetch to create log
        fetch_status, log = collector.fetch(url=url, refill_todays_logs=True)

        assert fetch_status == FetchStatus.FAILED, "initial mock should fail"

        # update to a successful response
        mock_response.status_code = 200

        # now run without refill_todays_logs to ensure log is not overwritten
        fetch_status, log = collector.fetch(url=url, refill_todays_logs=False)

        assert (
            fetch_status == FetchStatus.ALREADY_FETCHED
        ), "log should not be overwritten"
        mock_response.status_code = 200
        fetch_status, log = collector.fetch(url=url, refill_todays_logs=True)

        assert fetch_status == FetchStatus.OK
        assert log["endpoint-url"] == url
        assert log["status"] == "200"
        assert "resource" in log
        assert log["resource"] is not None

        # Check log file was created
        log_files = list(log_dir.rglob("*.json"))
        assert len(log_files) == 1

        # Read and verify log file content
        with open(log_files[0], "r") as f:
            saved_log = json.load(f)
            assert saved_log["endpoint-url"] == url
            assert saved_log["status"] == "200"

    def test_fetch_handles_non_200_status(self, collection_dir, tmp_path, mocker):
        """Test that fetch handles non-200 status codes correctly"""
        # -- Arrange --
        url = "https://example.com/not-found.csv"

        mock_response = create_mock_response(
            mocker, status_code=404, content=b"Not Found", content_type="text/html"
        )

        mocker.patch(
            "digital_land.collect.requests.Session.get",
            return_value=mock_response,
        )

        log_dir = Path(collection_dir) / "log"
        resource_dir = Path(collection_dir) / "resource"

        collector = Collector(resource_dir=str(resource_dir), log_dir=str(log_dir))

        # -- Act --
        fetch_status, log = collector.fetch(url=url)

        # -- Assert --
        assert fetch_status == FetchStatus.FAILED
        assert log["status"] == "404"
        assert "resource" not in log
