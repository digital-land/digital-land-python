import json
import pandas as pd
from digital_land.utils import la_geometry_processor


def test_prepare_la_geometry_data():
    with open("tests/data/boundary/la_geometry.geojson") as f:
        gj = json.load(f)
    df = la_geometry_processor.process_geojson(
        gj, "tests/data/boundary/organisation.csv"
    )
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
    # "local-authority-eng:RCC" should not be present in DF as its geometry is invalid
    assert "local-authority-eng:RCC" not in df.values
    assert len(df) == len(expected_df)
