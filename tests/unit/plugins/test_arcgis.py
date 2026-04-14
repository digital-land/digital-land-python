import pytest
import requests

from digital_land.plugins.arcgis import ArcGISParameters
from digital_land.plugins.arcgis import get as arcgis_get
from digital_land.plugins.arcgis import (
    validate_parameters as validate_arcgis_parameters,
)


def test_arcgis_get_returns_no_partial_string_on_iteration_failure(mocker, caplog):
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

    mocker.patch("digital_land.plugins.arcgis.EsriDumper", FakeDumper)

    with caplog.at_level("WARNING"):
        log, content = arcgis_get(
            None,
            "https://example.com/arcgis",
            parameters=ArcGISParameters(),
        )

    assert content is None
    assert log["status"] == "200"
    assert log["exception"] == "ReadTimeout"
    assert "ArcGIS fetch failed at step 'feature-iteration'" in caplog.text


def test_arcgis_validate_parameters_defaults():
    assert validate_arcgis_parameters({"max_page_size": 20}) == ArcGISParameters(
        max_page_size=20
    )


def test_arcgis_validate_parameters_rejects_unknown_parameters():
    with pytest.raises(ValueError, match="Invalid ArcGIS parameters"):
        validate_arcgis_parameters({"unknown": 1})


def test_arcgis_get_retries_and_logs_failed_step(mocker, caplog):
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

    mocker.patch("digital_land.plugins.arcgis.EsriDumper", FakeDumper)
    mocker.patch(
        "digital_land.plugins.arcgis.time.sleep", side_effect=sleep_calls.append
    )

    with caplog.at_level("WARNING"):
        log, content = arcgis_get(
            None,
            "https://example.com/arcgis",
            parameters=ArcGISParameters(
                retries=1,
                retry_backoff_seconds=3,
                timeout=90,
            ),
        )

    assert attempts["count"] == 2
    assert sleep_calls == [3]
    assert content is not None
    assert log["status"] == "200"
    assert "ArcGIS fetch failed at step 'metadata' on attempt 1/2" in caplog.text


def test_arcgis_get_rejects_unvalidated_parameter_dict():
    with pytest.raises(
        TypeError,
        match="ArcGIS get expects parameters to be an ArcGISParameters instance",
    ):
        arcgis_get(
            None,
            "https://example.com/arcgis",
            parameters={"max_page_size": 20},
        )
