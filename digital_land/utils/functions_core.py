import urllib
import sqlite3
import pandas as pd
import geopandas as gpd
import shapely.wkt
from pathlib import Path

global FILES_URL

FILES_URL = "https://datasette.planning.data.gov.uk/"


def download_dataset(dataset, output_dir_path, overwrite=False):
    output_dir = Path(output_dir_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_file_name = f"{dataset}.db"
    output_file_path = output_dir / dataset_file_name

    if not overwrite and output_file_path.exists():
        return

    final_url = f"{FILES_URL}{dataset_file_name}"
    print(f"downloading data from {final_url}")
    print(f"to: {output_file_path}")

    urllib.request.urlretrieve(final_url, output_file_path)
    print("download complete")


def get_pdp_dataset(
    dataset, geometry_field="geometry", crs_out=4326, underscore_cols=True
):

    df = pd.read_csv(
        f"https://files.planning.data.gov.uk/dataset/{dataset}.csv", dtype="str"
    )
    df.columns = [x.replace("-", "_") for x in df.columns]

    df_valid_geom = df[df[geometry_field].notnull()].copy()

    # load geometry and create GDF
    df_valid_geom[geometry_field] = df_valid_geom[geometry_field].apply(
        shapely.wkt.loads
    )
    gdf = gpd.GeoDataFrame(df_valid_geom, geometry=geometry_field)

    # Transform to ESPG:27700 for more interpretable area units
    gdf.set_crs(epsg=4326, inplace=True)
    gdf.to_crs(epsg=crs_out, inplace=True)

    return gdf


def query_sqlite(db_path, query_string):

    with sqlite3.connect(db_path) as con:

        cursor = con.execute(query_string)
        cols = [column[0] for column in cursor.description]
        results_df = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    return results_df


def datasette_query(db, sql_string):
    params = urllib.parse.urlencode({"sql": sql_string, "_size": "max"})
    url = f"https://datasette.planning.data.gov.uk/{db}.csv?{params}"
    df = pd.read_csv(url)
    return df
