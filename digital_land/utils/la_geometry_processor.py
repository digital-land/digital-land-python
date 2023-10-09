import json
import logging
from shapely.geometry import shape
import pandas as pd


def process_geojson(gj, organisation_path):
    rows_list = []
    # Extract geometry data from geojson
    for i in range(len(gj["features"])):
        statistical_geography = gj["features"][i]["attributes"]["LAD23CD"]
        rings = gj["features"][i]["geometry"]["rings"]
        geometry = {"type": "Polygon", "coordinates": rings}
        geom = shape(geometry)
        if geom.is_valid:
            new_row = {
                "statistical-geography": statistical_geography,
                "geometry": geom.wkt,
            }
            rows_list.append(new_row)

    # Match geometry data to organisation data
    df = pd.DataFrame(rows_list, columns=["statistical-geography", "geometry"])
    df_organisations = pd.read_csv(organisation_path)
    df = df.merge(df_organisations, on="statistical-geography", how="left")
    df.dropna(subset=["organisation"], inplace=True)
    df = df[["organisation", "geometry"]]
    return df


def prepare_la_geometry_data(organisation_path):
    # This method processes local authority geometry data downloaded from ONS
    # so it can be used for boundary checks in the harmonise phase
    try:
        with open("var/cache/la_geometry.geojson") as f:
            gj = json.load(f)
        df = process_geojson(gj, organisation_path)
        output_path = "var/cache/la_geometry.csv"
        df.to_csv(output_path)
        return output_path
    except FileNotFoundError:
        logging.error("Could not find Local Authority geometry geojson")
        return None
    except Exception:
        logging.error("Could not process Local Authority geometry data")
        return None
