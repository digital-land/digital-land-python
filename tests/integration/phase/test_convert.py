import pytest
import json
import pandas as pd
from pathlib import Path
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
