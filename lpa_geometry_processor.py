import urllib
import json
import requests
from shapely.geometry import shape
import pandas as pd


# osgb_to_wgs84 = Transformer.from_crs(27700, 4326, always_xy=True)


def get_lpa_geometry_data():
    # This script accepts the lpa geometry geojson found in the following URL and converts it into WKTs to be used as custom boundaries
    # url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Planning_Authorities_April_2023_Boundaries_UK_BFE/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson" # noqa: E501
    url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2023_UK_BFE_V2/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"  # noqa: E501
    response_API = requests.get(url)
    data = response_API.text
    gj = json.loads(data)

    rows_list = []
    for i in range(len(gj["features"])):
        name = gj["features"][i]["attributes"]["LAD23NM"]
        statistical_geography = gj["features"][i]["attributes"]["LAD23CD"]
        rings = gj["features"][i]["geometry"]["rings"]
        geometry = {"type": "Polygon", "coordinates": rings}
        geom = shape(geometry)

        # geom_wgs84 = transform(osgb_to_wgs84.transform, geom)
        new_row = {
            "name": name,
            "statistical_geography": statistical_geography,
            "geometry": geom.wkt,
        }
        rows_list.append(new_row)

    df = pd.DataFrame(rows_list, columns=["name", "statistical_geography", "geometry"])
    # df_organisations = get_organisations()
    # merge organisations df with existing df
    df.to_csv("lpa_geometry.csv")


# get_lpa_geometry_data()


def get_organisations():
    params = urllib.parse.urlencode(
        {
            "sql": """
        select organisation, statistical_geography
        from organisation
        """,
            "_size": "max",
        }
    )
    url = f"https://datasette.planning.data.gov.uk/digital-land.csv?{params}"
    df = pd.read_csv(url)
    return df


def get_statistical_geography(organisation):
    params = urllib.parse.urlencode(
        {
            "sql": f"""
        select statistical_geography
        from organisation
        where organisation = '{organisation}'
        """,
            "_size": "max",
        }
    )
    url = f"https://datasette.planning.data.gov.uk/digital-land.csv?{params}"
    df = pd.read_csv(url)
    try:
        print("Found statistical geography: " + df.loc[0, "statistical_geography"])
        return df.loc[0, "statistical_geography"]
    except KeyError:
        return None


def get_lpa_geometry(organisation):
    statistical_geography = get_statistical_geography(organisation)
    if statistical_geography:
        df = pd.read_csv("lpa_geometry.csv")
        try:
            df.loc[
                df["statistical_geography"] == statistical_geography, "geometry"
            ].to_csv("polygon.csv")
            return df.loc[
                df["statistical_geography"] == statistical_geography, "geometry"
            ].iloc[0]
        except KeyError:
            return None
    else:
        return None


print(get_lpa_geometry("local-authority-eng:YOR"))
