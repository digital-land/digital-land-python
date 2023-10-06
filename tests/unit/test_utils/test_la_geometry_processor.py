import json
import pandas as pd
from digital_land.utils import la_geometry_processor


def test_prepare_la_geometry_data():
    with open("tests/data/boundary/la_geometry.geojson") as f:
        gj = json.load(f)
    df = la_geometry_processor.process_data(gj, "tests/data/boundary/organisation.csv")
    expected_df = pd.read_csv("tests/data/boundary/la_geometry.csv")
    assert (
        df.loc[df["organisation"] == "local-authority-eng:HPL"].geometry.values[0]
        == expected_df.loc[
            expected_df["organisation"] == "local-authority-eng:HPL"
        ].geometry.values[0]
    )
    assert (
        df.loc[df["organisation"] == "local-authority-eng:MDB"].geometry.values[0]
        == expected_df.loc[
            expected_df["organisation"] == "local-authority-eng:MDB"
        ].geometry.values[0]
    )
    assert (
        df.loc[df["organisation"] == "local-authority-eng:RCC"].geometry.values[0]
        == expected_df.loc[
            expected_df["organisation"] == "local-authority-eng:RCC"
        ].geometry.values[0]
    )
