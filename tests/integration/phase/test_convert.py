import pytest
import json
import pandas as pd
from pathlib import Path
from digital_land.phase.convert import (
    ConvertPhase,
    detect_encoding,
    detect_file_encoding,
)

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


def test_detect_file_encoding_utf8_without_bom(tmp_path):
    # UTF-8 multibyte sequences are structurally unique — unambiguous detection
    path = tmp_path / "utf8.csv"
    path.write_bytes("reference,name\nA1,Ångström Road\nA2,Héraclès\n".encode("utf-8"))

    result = detect_file_encoding(str(path))

    assert result is not None
    assert "utf" in result.lower()


def test_detect_file_encoding_utf8_with_bom(tmp_path):
    # BOM makes encoding completely unambiguous
    path = tmp_path / "utf8bom.csv"
    path.write_bytes(b"\xef\xbb\xbf" + "reference,name\nA1,Café\n".encode("utf-8"))

    result = detect_file_encoding(str(path))

    assert result is not None
    assert "utf" in result.lower()


def test_detect_file_encoding_utf16(tmp_path):
    # UTF-16 BOM is completely unambiguous
    path = tmp_path / "utf16.csv"
    path.write_bytes("reference,name\nA1,Café\n".encode("utf-16"))

    result = detect_file_encoding(str(path))

    assert result is not None
    assert "utf" in result.lower()


@pytest.mark.parametrize(
    "encoding",
    ["latin-1", "windows-1252"],
)
def test_detect_file_encoding_single_byte_returns_usable_encoding(encoding, tmp_path):
    # latin-1, cp1252, and cp1250 share most byte values so detection is
    # statistical — the contract is that the returned encoding can open the file
    content = "reference,name\nA1,Café Street\nA2,Ångström Road\n"
    path = tmp_path / "test.csv"
    path.write_bytes(content.encode(encoding))

    result = detect_file_encoding(str(path))

    assert result is not None
    path.read_bytes().decode(result)


def test_detect_encoding_from_file_object(tmp_path):
    # Verify the file-object variant works — use UTF-8 which is unambiguous
    content = "reference,name\nA1,Ångström Road\nA2,Héraclès\n"
    path = tmp_path / "utf8.csv"
    path.write_bytes(content.encode("utf-8"))

    with open(path, "rb") as f:
        result = detect_encoding(f)

    assert result is not None
    assert "utf" in result.lower()


def test_detect_file_encoding_empty_file_returns_a_usable_encoding(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_bytes(b"")

    result = detect_file_encoding(str(path))

    # charset-normalizer returns utf_8 for empty files; callers must handle None too
    assert result is None or "utf" in result.lower()
