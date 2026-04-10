import hashlib
import io
import json
import csv
import os
import pathlib
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import pytest
import responses
import requests

from digital_land.collect import Collector, FetchStatus
from digital_land.plugins.arcgis import get as arcgis_get
from digital_land.plugins.arcgis import validate_parameters as validate_arcgis_parameters


@pytest.fixture
def collector(tmp_path):
    collector = Collector(
        resource_dir=str(tmp_path / "resource"), log_dir=str(tmp_path / "log")
    )
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


def test_no_resource_dir_raises_error():
    with pytest.raises(ValueError, match="resource_dir must be set and not empty"):
        Collector(resource_dir=None)


def test_empty_resource_dir_raises_error():
    with pytest.raises(ValueError, match="resource_dir must be set and not empty"):
        Collector(resource_dir="")


@responses.activate
def test_fetch(collector, prepared_response, tmp_path):
    url = "http://some.url"
    status, log = collector.fetch(url)

    assert status == FetchStatus.OK
    assert log is not None, "Log should not be None after successful fetch"
    output_path = tmp_path / f"resource/{sha_digest('some data')}"
    assert os.path.isfile(output_path)
    assert open(output_path).read() == "some data"
    assert os.path.isfile(pathlib.Path(collector.log_dir) / log_file(url))


@responses.activate
def test_already_fetched(collector, prepared_response):
    status, log = collector.fetch("http://some.url")
    assert status == FetchStatus.OK
    assert log is not None, "Log should not be None after successful fetch"

    new_status, new_log = collector.fetch("http://some.url")
    assert new_status == FetchStatus.ALREADY_FETCHED
    assert new_log is None, "Log should be None when already fetched"


@responses.activate
def test_refill_todays_logs(collector, prepared_response):
    status, log = collector.fetch("http://some.url")
    assert status == FetchStatus.OK
    assert log is not None, "Log should not be None after successful fetch"

    new_status, new_log = collector.fetch("http://some.url", refill_todays_logs=True)
    assert new_status == FetchStatus.OK
    assert new_log is not None, "Log should not be None after successful fetch"


@responses.activate
def test_expired(collector):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    status, log = collector.fetch("http://some.url", end_date=yesterday)

    assert status == FetchStatus.EXPIRED
    assert log is None, "Log should be None when expired"


@responses.activate
def test_hash_check(collector, prepared_response):
    url = "http://some.url"
    status, log = collector.fetch(url, endpoint=sha_digest(url))

    assert log is not None, "Log should not be None after successful fetch"
    assert status == FetchStatus.OK


@responses.activate
def test_hash_failure(collector, prepared_response):
    status, log = collector.fetch("http://some.url", endpoint="http://other.url")

    assert log is None, "Log should not be None after hash failure fetch"
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

    fetch_status, log = collector.fetch(url)

    assert fetch_status == FetchStatus.OK
    # Check that the timestamp is removed
    expected_content = '{"data": "some data"}'
    expected_hash = sha_digest(expected_content)
    output_path = tmp_path / f"resource/{expected_hash}"

    assert os.path.isfile(output_path)
    assert open(output_path).read() == expected_content

    log_content = read_log(collector, url)
    assert log_content["resource"] == expected_hash


@responses.activate
def test_strip_timestamp_xml(collector, tmp_path):
    url = "http://test.timestamp"
    xml_with_timestamp = (
        '<root timeStamp="2024-10-02T13:41:59Z" numberMatched="unknown" />'
    )
    xml_parsed = ET.fromstring(xml_with_timestamp)

    responses.add(
        responses.GET,
        url,
        body=ET.tostring(xml_parsed, encoding="unicode"),
        content_type="application/xml;charset=UTF-8",
    )

    fetch_status, log = collector.fetch(url)

    assert fetch_status == FetchStatus.OK
    # Check that the timestamp is removed
    expected_content = '<root numberMatched="unknown" />'
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


def test_arcgis_get_returns_no_partial_string_on_iteration_failure(monkeypatch):
    class FakeResponse:
        status_code = 200

    class FakeDumper:
        def __init__(self, *args, **kwargs):
            pass

        def _request(self, method, url):
            return FakeResponse()

        def get_metadata(self):
            return None

        def __iter__(self):
            raise requests.ReadTimeout("timed out")

    monkeypatch.setattr("digital_land.plugins.arcgis.EsriDumper", FakeDumper)

    log, content = arcgis_get(None, "https://example.com/arcgis")

    assert content is None
    assert log["status"] == "200"
    assert log["exception"] == "ReadTimeout"
    assert log["arcgis-failed-step"] == "feature-iteration"


def test_arcgis_validate_parameters_defaults():
    assert validate_arcgis_parameters({"max_page_size": 20}) == {
        "max_page_size": 20,
        "timeout": 60,
        "retries": 2,
        "retry_backoff_seconds": 2,
    }


def test_arcgis_get_retries_and_logs_failed_step(monkeypatch):
    attempts = {"count": 0}
    sleep_calls = []

    class FakeResponse:
        status_code = 200

    class FakeDumper:
        def __init__(self, *args, **kwargs):
            pass

        def _request(self, method, url):
            return FakeResponse()

        def get_metadata(self):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise requests.ReadTimeout("metadata timed out")

        def __iter__(self):
            yield {"type": "Feature", "properties": {}, "geometry": None}

    monkeypatch.setattr("digital_land.plugins.arcgis.EsriDumper", FakeDumper)
    monkeypatch.setattr("digital_land.plugins.arcgis.time.sleep", sleep_calls.append)

    log, content = arcgis_get(
        None,
        "https://example.com/arcgis",
        parameters={"retries": 1, "retry_backoff_seconds": 3, "timeout": 90},
    )

    assert attempts["count"] == 2
    assert sleep_calls == [3]
    assert content is not None
    assert log["status"] == "200"
    assert log["arcgis-attempt"] == 2
    assert log["arcgis-retried"] == 1
    assert log["arcgis-failed-step"] == "metadata"
    assert log["arcgis-timeout-seconds"] == 90
    assert log["arcgis-retries"] == 1
    assert log["arcgis-retry-backoff-seconds"] == 3
    assert "exception" not in log  
    
def test_fetch_passes_parameters_to_arcgis_plugin(collector, monkeypatch):
    captured = {}

    def fake_arcgis_get(collector_obj, url, log, parameters=None, plugin="arcgis"):
        captured["collector"] = collector_obj
        captured["url"] = url
        captured["parameters"] = parameters
        return log, b'{"type":"FeatureCollection","features":[]}'

    monkeypatch.setattr("digital_land.collect.arcgis_get", fake_arcgis_get)

    url = "http://some.arcgis.url"
    status, log = collector.fetch(
        url,
        endpoint=sha_digest(url),
        plugin="arcgis",
        parameters={"max_page_size": 20},
        
        refill_todays_logs=True,
    )

    assert status == FetchStatus.OK
    assert captured["url"] == url
    assert captured["parameters"] == {"max_page_size": 20}
    
def test_collect_reads_parameters_from_csv(tmp_path, monkeypatch):
    collector = Collector(
        resource_dir=str(tmp_path / "resource"),
        log_dir=str(tmp_path / "log"),
    )

    url = "http://some.arcgis.url"
    endpoint = sha_digest(url)

    endpoint_csv = tmp_path / "endpoints.csv"
    with open(endpoint_csv, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["endpoint", "endpoint-url", "plugin", "parameters"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": endpoint,
                "endpoint-url": url,
                "plugin": "arcgis",
                "parameters": '{"max_page_size": 20}',
            }
        )

    captured = {}

    def fake_fetch(
        self,
        url,
        endpoint=None,
        log_datetime=datetime.utcnow(),
        end_date="",
        plugin="",
        parameters=None,
        refill_todays_logs=False,
    ):
        captured["url"] = url
        captured["endpoint"] = endpoint
        captured["plugin"] = plugin
        captured["parameters"] = parameters
        return FetchStatus.OK, {"status": "200"}

    monkeypatch.setattr(Collector, "fetch", fake_fetch)

    collector.collect(endpoint_csv)

    assert captured["url"] == url
    assert captured["endpoint"] == endpoint
    assert captured["plugin"] == "arcgis"
    assert captured["parameters"] == {"max_page_size": 20}
    
    
def test_collect_raises_for_invalid_parameters_json(tmp_path):
    collector = Collector(
        resource_dir=str(tmp_path / "resource"),
        log_dir=str(tmp_path / "log"),
    )

    url = "http://some.arcgis.url"
    endpoint = sha_digest(url)

    endpoint_csv = tmp_path / "endpoints.csv"
    with open(endpoint_csv, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["endpoint", "endpoint-url", "plugin", "parameters"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": endpoint,
                "endpoint-url": url,
                "plugin": "arcgis",
                "parameters": "{max_page_size: 20}",  # invalid JSON
            }
        )

    with pytest.raises(ValueError, match="Invalid parameters JSON"):
        collector.collect(endpoint_csv)
