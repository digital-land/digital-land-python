from subprocess import CompletedProcess

from digital_land.plugins.wfs import WFSFileResource
from digital_land.plugins.wfs import WFSParameters
from digital_land.plugins.wfs import get as wfs_get


def test_get_falls_back_to_default_parameters_for_unvalidated_parameter_dict(caplog):
    class FakeCollector:
        def get(self, url, log, plugin):
            log["status"] = "200"
            return log, b"reference"

    with caplog.at_level("WARNING"):
        log, content = wfs_get(
            FakeCollector(),
            "https://example.com/wfs",
            parameters={"paging": True},
        )

    assert log["status"] == "200"
    assert content == b"reference"
    assert (
        "WFS get expects parameters to be a WFSParameters instance. using default parameters"
        in caplog.text
    )


def test_get_paged_wfs_runs_ogr2ogr_with_paging_config(tmp_path, mocker):
    output_path = tmp_path / "output.gpkg"
    captured = {}

    class FakeTempFile:
        name = str(output_path)

        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    def fake_run(command, capture_output, check):
        captured["command"] = command
        captured["capture_output"] = capture_output
        captured["check"] = check
        output_path.write_bytes(b"geopackage")
        return CompletedProcess(command, 0, stdout=b"", stderr=b"")

    mocker.patch("digital_land.plugins.wfs.tempfile.NamedTemporaryFile", FakeTempFile)
    mocker.patch("digital_land.plugins.wfs.subprocess.run", side_effect=fake_run)

    log, content = wfs_get(
        None,
        "https://example.com/wfs?request=GetFeature",
        parameters=WFSParameters(paging=True, page_size=500),
    )

    assert isinstance(content, WFSFileResource)
    assert content.path == str(output_path)
    assert log["status"] == "200"
    assert captured["command"] == [
        "ogr2ogr",
        "--config",
        "OGR_WFS_PAGING_ALLOWED",
        "ON",
        "--config",
        "OGR_WFS_PAGING_PAGE_SIZE",
        "500",
        "-f",
        "GPKG",
        str(output_path),
        "WFS:https://example.com/wfs?request=GetFeature",
    ]

def test_get_paged_wfs_logs_failure_and_cleans_temp_file(tmp_path, mocker):
    output_path = tmp_path / "output.gpkg"

    class FakeTempFile:
        name = str(output_path)

        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    def fake_run(command, capture_output, check):
        output_path.write_bytes(b"partial")
        return CompletedProcess(command, 1, stdout=b"", stderr=b"failed")

    mocker.patch("digital_land.plugins.wfs.tempfile.NamedTemporaryFile", FakeTempFile)
    mocker.patch("digital_land.plugins.wfs.subprocess.run", side_effect=fake_run)

    log, content = wfs_get(
        None,
        "https://example.com/wfs",
        parameters=WFSParameters(paging=True),
    )

    assert content is None
    assert log["status"] == "1"
    assert log["exception"] == "CalledProcessError"
    assert not output_path.exists()


def test_get_paged_wfs_can_use_source_url_override_and_layer_name(tmp_path, mocker):
    output_path = tmp_path / "output.gpkg"
    captured = {}

    class FakeTempFile:
        name = str(output_path)

        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass

    def fake_run(command, capture_output, check):
        captured["command"] = command
        output_path.write_bytes(b"geopackage")
        return CompletedProcess(command, 0, stdout=b"", stderr=b"")

    mocker.patch("digital_land.plugins.wfs.tempfile.NamedTemporaryFile", FakeTempFile)
    mocker.patch("digital_land.plugins.wfs.subprocess.run", side_effect=fake_run)

    log, content = wfs_get(
        None,
        "https://example.com/wfs?request=GetFeature&typeName=ignored",
        parameters=WFSParameters(
            paging=True,
            source_url="https://example.com/wfs",
            layer_name="dataset:Flood_Zones",
        ),
    )

    assert isinstance(content, WFSFileResource)
    assert log["status"] == "200"
    assert captured["command"][-2:] == [
        "WFS:https://example.com/wfs",
        "dataset:Flood_Zones",
    ]
