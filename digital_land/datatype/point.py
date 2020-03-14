#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log fixes as suggestions for the user to amend
#

from decimal import Decimal
from .decimal import format_decimal
from .datatype import DataType

from pyproj import Transformer

# convert from OSGB to WGS84
# https://epsg.io/27700
# https://epsg.io/4326
osgb_to_wgs84 = Transformer.from_crs(27700, 4326)


def degrees_like(lon, lat):
    return lat > -60.0 and lat < 60.0 and lon > -60.0 and lon < 60.0


def easting_northing_like(lon, lat):
    return lat > 1000.0 and lat < 1000000.0 and lon > 1000.0 and lon < 1000000.0


def within_england(lon, lat):
    return lon > -7.0 and lon < 2 and lat > 49.5 and lat < 56.0


class PointDataType(DataType):
    def normalise(self, values, default=["", ""], issues=None):
        if "" in values:
            return default

        (lon, lat) = [Decimal(value) for value in values]
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
        else:
            if issues:
                issues.log("out of range", value)
            return default

        return [format_decimal(lon), format_decimal(lat)]
