import pandas as pd

from digital_land.pipeline.process import convert_tranformed_csv_to_pq


def test_convert_transformed_csv_to_pq_converts_csv(tmp_path):
    # creat csv that looks like a transformed csv
    data = {
        "end-date": [""],
        "entity": [4220000],
        "entry-date": ["2024-10-02"],
        "entry-number": [1],
        "fact": ["1be8ef923db61d62354f041718ea0b1795c5ae60b436ec74e90d9fd850919434"],
        "field": ["name"],
        "priority": [2],
        "reference-entity": [""],
        "resource": [
            "0d1f06295866286d290d831b4569fe862ab38ca72cd23d541de2c9f20ff44ed7"
        ],
        "start-date": [""],
        "value": "Arun District Council Local Plan 2011 - 2031",
    }
    df = pd.DataFrame(data)
    data_path = (
        tmp_path
        / "0d1f06295866286d290d831b4569fe862ab38ca72cd23d541de2c9f20ff44ed7.csv"
    )
    df.to_csv(data_path)

    # use process on it
    output_path = (
        tmp_path
        / "0d1f06295866286d290d831b4569fe862ab38ca72cd23d541de2c9f20ff44ed7.parquet"
    )
    convert_tranformed_csv_to_pq(
        data_path,
        tmp_path
        / "0d1f06295866286d290d831b4569fe862ab38ca72cd23d541de2c9f20ff44ed7.parquet",
    )

    # check resulting parquet file for:
    assert (
        output_path.exists()
    ), f"no parquet file created as expected at {str(output_path)}"
    # headers and number of rows
    parquet_df = pd.read_parquet(output_path)
    for col in list(parquet_df.columns):
        assert "-" not in col

    for col in list(df.columns):
        assert col.replace("-", "_") in list(parquet_df.columns)


# check column types
