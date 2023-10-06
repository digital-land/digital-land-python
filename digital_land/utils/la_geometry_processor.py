# import urllib
import json
import logging

# import requests
# import os
from shapely.geometry import shape
import pandas as pd


# osgb_to_wgs84 = Transformer.from_crs(27700, 4326, always_xy=True)
# def get_organisations():
#     params = urllib.parse.urlencode(
#         {
#             "sql": """
#         select organisation, statistical_geography
#         from organisation
#         """,
#             "_size": "max",
#         }
#     )
#     url = f"https://datasette.planning.data.gov.uk/digital-land.csv?{params}"
#     df = pd.read_csv(url)
#     return df


def prepare_la_geometry_data():
    # This script accepts the lpa geometry geojson found in the following URL and converts it into WKTs to be used as custom boundaries
    # url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Planning_Authorities_April_2023_Boundaries_UK_BFE/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson" # noqa: E501
    # url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2023_UK_BFE_V2/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"  # noqa: E501
    # response_API = requests.get(url)
    # if (response_API.text):
    #     data = response_API.text
    #     gj = json.loads(data)
    #     print(gj)
    try:
        with open("var/cache/la_geometry.geojson") as f:
            gj = json.load(f)
        rows_list = []
        for i in range(len(gj["features"])):
            statistical_geography = gj["features"][i]["attributes"]["LAD23CD"]
            rings = gj["features"][i]["geometry"]["rings"]
            geometry = {"type": "Polygon", "coordinates": rings}
            geom = shape(geometry)

            # geom_wgs84 = transform(osgb_to_wgs84.transform, geom)
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
    except Exception as e:
        logging.error("Could not process Local Authority geometry data", e)
        return None
    # else:
    #     print("Error fetching geometry data: ", response_API.status_code)


# def get_lpa_geometry(organisation):

#     df = pd.read_csv("lpa_geometry.csv")
#     try:
#         df.loc[
#             df["statistical_geography"] == organisation, "geometry"
#         ].to_csv("polygon.csv")
#         return df.loc[
#             df["statistical_geography"] == organisation, "geometry"
#         ].iloc[0]
#     except KeyError:
#         return None

prepare_la_geometry_data()
# print(get_lpa_geometry("local-authority-eng:YOR"))
