import hashlib
import json
import os
import pathlib
from datetime import datetime, timedelta

import pytest
import responses
import requests

from digital_land.collect import Collector, FetchStatus


@pytest.fixture
def collector(tmp_path):
    collector = Collector()
    collector.resource_dir = str(tmp_path / "resource")
    collector.log_dir = str(tmp_path / "log")
    return collector


@pytest.fixture
def prepared_response():
    responses.add(responses.GET, "http://some.url", body="some data")


@pytest.fixture
def prepared_exception(exception_class):
    responses.add(
        responses.GET,
        "http://mock.url",
        body=exception_class(f"Test {exception_class.__name__} Error"),
    )


def sha_digest(string):
    return hashlib.sha256(string.encode("utf-8")).hexdigest()


@responses.activate
def test_fetch(collector, prepared_response, tmp_path):
    url = "http://some.url"
    status = collector.fetch(url)

    assert status == FetchStatus.OK
    output_path = tmp_path / f"resource/{sha_digest('some data')}"
    assert os.path.isfile(output_path)
    assert open(output_path).read() == "some data"
    assert os.path.isfile(pathlib.Path(collector.log_dir) / log_file(url))


@responses.activate
def test_already_fetched(collector, prepared_response):
    status = collector.fetch("http://some.url")
    assert status == FetchStatus.OK

    new_status = collector.fetch("http://some.url")
    assert new_status == FetchStatus.ALREADY_FETCHED


@responses.activate
def test_expired(collector):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    status = collector.fetch("http://some.url", end_date=yesterday)

    assert status == FetchStatus.EXPIRED


@responses.activate
def test_hash_check(collector, prepared_response):
    url = "http://some.url"
    status = collector.fetch(url, endpoint=sha_digest(url))

    assert status == FetchStatus.OK


@responses.activate
def test_hash_failure(collector, prepared_response):
    status = collector.fetch("http://some.url", endpoint="http://other.url")

    assert status == FetchStatus.HASH_FAILURE


def read_log(collector, url):
    return json.load(open(pathlib.Path(collector.log_dir) / log_file(url)))


def log_file(url):
    return f"{datetime.now().strftime('%Y-%m-%d')}/{sha_digest(url)}.json"


@responses.activate
@pytest.mark.parametrize(
    "exception_class",
    [
        requests.exceptions.SSLError,
        requests.ConnectionError,
        requests.HTTPError,
        requests.Timeout,
        requests.TooManyRedirects,
        requests.exceptions.MissingSchema,
        requests.exceptions.ChunkedEncodingError,
        requests.ConnectionError,
        requests.exceptions.ContentDecodingError,
    ],
)
def test_get(collector, prepared_exception, exception_class):
    url = "http://mock.url"
    log, content = collector.get(url)

    assert "status" not in log
    assert log.get("exception") == exception_class.__name__
    assert content is None


@responses.activate
def test_strip_timestamp(collector, tmp_path):
    url = "http://test.timestamp"
    json_with_timestamp = '{"data": "some data", "timeStamp": "2023-06-03T12:00:00Z"}'
    responses.add(
        responses.GET,
        url,
        json=json.loads(json_with_timestamp),
        content_type="application/json",
    )

    status = collector.fetch(url)

    assert status == FetchStatus.OK
    # Check that the timestamp is removed
    expected_content = '{"data": "some data"}'
    expected_hash = sha_digest(expected_content)
    output_path = tmp_path / f"resource/{expected_hash}"

    assert os.path.isfile(output_path)
    assert open(output_path).read() == expected_content

    log_content = read_log(collector, url)
    assert log_content["resource"] == expected_hash


def test_save_resource(collector, tmp_path):
    log = {}
    url = "http://some.url"
    status = collector.save_resource(b"some data", url, log)

    assert status == FetchStatus.OK
    output_path = tmp_path / f"resource/{sha_digest('some data')}"
    assert os.path.isfile(output_path)
    assert open(output_path).read() == "some data"


def test_resource_not_bytes(collector):
    log = {}
    url = "http://other.url"
    status = collector.save_resource("Unicode data", url, log)

    assert status == FetchStatus.FAILED
    assert log["exception"] == "TypeError"
