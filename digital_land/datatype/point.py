#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log fixes as suggestions for the user to amend
#

import decimal
from .decimal import DecimalDataType
from .datatype import DataType

from pyproj import Transformer

# convert from OSGB to WGS84
# https://epsg.io/27700
# https://epsg.io/4326
osgb_to_wgs84 = Transformer.from_crs(27700, 4326)

# convert from OSM to WGS84
# https://epsg.io/3857
# https://epsg.io/4326
osm_to_wgs84 = Transformer.from_crs(3857, 4326)


def degrees_like(lon, lat):
    return lat > -60.0 and lat < 60.0 and lon > -60.0 and lon < 60.0


def easting_northing_like(lon, lat):
    return lat > 1000.0 and lat < 1000000.0 and lon > 1000.0 and lon < 1000000.0


def osm_like(lon, lat):
    return lat > 6000000.0 and lat < 10000000.0 and lon > -80000.0 and lon < 260000.0


def within_england(lon, lat):
    return lon > -7.0 and lon < 2 and lat > 49.5 and lat < 56.0


class PointDataType(DataType):
    def __init__(self, precision=6):
        self.decimal = DecimalDataType(precision=precision)

    def normalise(self, values, default=["", ""], issues=None):

        # assumes values have both been decimal normalised
        if "" in values:
            return default

        try:
            (lon, lat) = [decimal.Decimal(value) for value in values]
        except decimal.InvalidOperation:
            if issues:
                issues.log("Failed to convert point to decimal %s", values)
            return default

        value = ",".join(values)

        if degrees_like(lon, lat):
            if not within_england(lon, lat):
                lon, lat = lat, lon
                if not within_england(lon, lat):
                    if issues:
                        issues.log("WGS84 outside England", value)
                    return default
        elif easting_northing_like(lon, lat):
            lat, lon = osgb_to_wgs84.transform(lon, lat)
            if not within_england(lon, lat):
                lat, lon = osgb_to_wgs84.transform(lat, lon)
                if not within_england(lon, lat):
                    if issues:
                        issues.log("OSGB outside England", value)
                    return default
        elif osm_like(lon, lat):
            lat, lon = osm_to_wgs84.transform(lon, lat)
            if not within_england(lon, lat):
                lat, lon = osm_to_wgs84.transform(lat, lon)
                if not within_england(lon, lat):
                    if issues:
                        issues.log("OSM outside England", value)
                    return default
        else:
            if issues:
                issues.log("out of range", value)
            return default

        return [self.decimal.format(lon), self.decimal.format(lat)]
