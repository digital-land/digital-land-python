import json
import logging
from shapely.geometry import shape
import pandas as pd


def prepare_la_geometry_data():
    try:
        with open("var/cache/la_geometry.geojson") as f:
            gj = json.load(f)
        rows_list = []
        for i in range(len(gj["features"])):
            statistical_geography = gj["features"][i]["attributes"]["LAD23CD"]
            rings = gj["features"][i]["geometry"]["rings"]
            geometry = {"type": "Polygon", "coordinates": rings}
            geom = shape(geometry)
            new_row = {
                "statistical-geography": statistical_geography,
                "geometry": geom.wkt,
            }
            rows_list.append(new_row)

        df = pd.DataFrame(rows_list, columns=["statistical-geography", "geometry"])
        df_organisations = pd.read_csv("var/cache/organisation.csv")
        df = df.merge(df_organisations, on="statistical-geography", how="left")
        df = df[["organisation", "geometry"]]
        output_path = "var/cache/la_geometry.csv"
        df.to_csv(output_path)
        return output_path
    except FileNotFoundError:
        logging.error("Could not find Local Authority geometry geojson")
        return None
    except Exception:
        logging.error("Could not process Local Authority geometry data")
        return None
