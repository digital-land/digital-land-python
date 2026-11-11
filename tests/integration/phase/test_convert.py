import http.server
import json
import threading
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from digital_land.log import ConvertedResourceLog, DatasetResourceLog
from digital_land.phase.convert import ConvertPhase

# the convert phase ran even though the input file didn't exist might need another test
# this is a problem because a sqlite file will be made otherwise


@pytest.mark.parametrize(
    "input_data",
    [
        [
            {"reference": "A4_011", "name": "Vicarage Lane"},
            {"reference": "A4_006", "name": "Ashleigh Road"},
        ],
    ],
)
def test_convert_phase_process_converts_a_json_array(input_data, tmp_path: Path):
    """
    Test what happens when we use the convert phase on a jsonn array
    """

    input_path = tmp_path / "input.json"
    with open(input_path, "w", encoding="utf-8") as f:
        json.dump(input_data, f, ensure_ascii=False)

    output_path = tmp_path / "output.csv"

    convert_phase = ConvertPhase(
        path=input_path,
        output_path=output_path,
    )

    input_df = pd.read_json(input_path)

    convert_phase.process()
    assert output_path.exists(), "the output file hasn't been made"

    output_df = pd.read_csv(output_path)
    assert len(input_df) == len(
        output_df
    ), "the number of rows in the input and output files should be the same"


@pytest.fixture
def http_server():
    """A local HTTP server that records the paths it is asked for."""
    requested = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            requested.append(self.path)
            self.send_response(404)
            self.end_headers()

        do_HEAD = do_GET

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", requested
    server.shutdown()


def _convert(input_path, output_path):
    lines = ConvertPhase(
        path=str(input_path),
        dataset_resource_log=DatasetResourceLog(),
        converted_resource_log=ConvertedResourceLog(),
        output_path=str(output_path),
    ).process()
    return [block["line"] for block in lines]


def _vrt(source):
    return (
        '<?xml version="1.0"?>\n'
        '<OGRVRTDataSource><OGRVRTLayer name="layer">'
        f"<SrcDataSource>{source}</SrcDataSource>"
        '<SrcSQL dialect="sqlite">SELECT * FROM local</SrcSQL>'
        "</OGRVRTLayer></OGRVRTDataSource>\n"
    )


@pytest.mark.parametrize("packaging", ["xml", "zipped-gml"])
def test_convert_does_not_open_files_named_in_a_vrt(packaging, tmp_path: Path):
    local_file = tmp_path / "local.csv"
    local_file.write_text("name\nLOCAL-FILE-CONTENT\n")

    if packaging == "xml":
        input_path = tmp_path / "resource"
        input_path.write_text(_vrt(local_file))
    else:
        input_path = tmp_path / "resource.zip"
        with zipfile.ZipFile(input_path, "w") as archive:
            archive.writestr("data.gml", _vrt(local_file))

    output_path = tmp_path / "output.csv"
    lines = _convert(input_path, output_path)

    assert "LOCAL-FILE-CONTENT" not in str(lines)
    assert (
        not output_path.exists() or "LOCAL-FILE-CONTENT" not in output_path.read_text()
    )


def test_convert_does_not_fetch_urls_named_in_a_file(
    http_server, monkeypatch, tmp_path: Path
):
    url, requested = http_server
    # NO_PROXY must not let GDAL bypass the blocked network
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    input_path = tmp_path / "resource.geojson"
    input_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "link", "properties": {"href": f"{url}/crs"}},
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"reference": "A1"},
                        "geometry": {"type": "Point", "coordinates": [-1.5, 52.5]},
                    }
                ],
            }
        )
    )

    lines = _convert(input_path, tmp_path / "output.csv")

    assert requested == []
    assert "A1" in str(lines)
