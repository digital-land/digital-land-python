from digital_land.phase.map import MapPhase
import logging


def test_process_multiple_columns_for_one_mapping():
    input_stream = [
        {
            "row": {
                "WKT": "test1",
                "geometry": "",
            }
        }
    ]
    fieldnames = ["geometry"]
    columns = {"wkt": "geometry"}
    phase = MapPhase(fieldnames=fieldnames, columns=columns, log=None)
    output = [block for block in phase.process(input_stream)]
    logging.warning(output[0])
    assert False
