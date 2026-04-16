import requests

from digital_land.plugins.arcgis import ArcGISParameters
from digital_land.plugins.arcgis import get as arcgis_get


def test_arcgis_get_returns_partial_string_on_iteration_failure(mocker, caplog):
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

    assert content == '{"type":"FeatureCollection","features":['
    assert log["exception"] == "ReadTimeout"
    assert "timed out" in caplog.text


def test_arcgis_get_passes_retry_configuration_to_esri_dumper(mocker, caplog):
    attempts = {"count": 0}
    init_kwargs = {}

    class FakeResponse:
        status_code = 200

    class FakeDumper:
        def __init__(self, *args, **kwargs):
            init_kwargs.update(kwargs)

        def _request(self, method, url):
            return FakeResponse()

        def get_metadata(self):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise requests.ReadTimeout("metadata timed out")

        def __iter__(self):
            yield {"type": "Feature", "properties": {}, "geometry": None}

    mocker.patch("digital_land.plugins.arcgis.EsriDumper", FakeDumper)

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

    assert attempts["count"] == 1
    assert content is None
    assert log["exception"] == "ReadTimeout"
    assert init_kwargs == {
        "fields": None,
        "max_page_size": None,
        "timeout": 90,
        "pause_seconds": 3,
        "num_of_retry": 1,
    }
    assert "metadata timed out" in caplog.text


def test_arcgis_get_falls_back_to_default_parameters_for_unvalidated_parameter_dict(
    mocker, caplog
):
    init_kwargs = {}

    class FakeResponse:
        status_code = 200

    class FakeDumper:
        def __init__(self, *args, **kwargs):
            init_kwargs.update(kwargs)

        def _request(self, method, url):
            return FakeResponse()

        def get_metadata(self):
            return None

        def __iter__(self):
            yield {"type": "Feature", "properties": {}, "geometry": None}

    mocker.patch("digital_land.plugins.arcgis.EsriDumper", FakeDumper)

    with caplog.at_level("WARNING"):
        log, content = arcgis_get(
            None,
            "https://example.com/arcgis",
            parameters={"max_page_size": 20},
        )

    assert content is not None
    assert log["status"] == "200"
    assert init_kwargs == {
        "fields": None,
        "max_page_size": None,
        "timeout": 60,
        "pause_seconds": 2,
        "num_of_retry": 2,
    }
    assert (
        "ArcGIS get expects parameters to be an ArcGISParameters instance. using default parameters"
        in caplog.text
    )
