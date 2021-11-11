import shapely.wkt
from shapely.ops import transform
from shapely.geometry import MultiPolygon

from .datatype import DataType
from .point import (
    degrees_like,
    easting_northing_like,
    osm_like,
    osgb_to_wgs84,
    osm_to_wgs84,
    within_england,
)


def flip(x, y, z=None):
    return tuple(filter(None, [y, x, z]))


class WktDataType(DataType):
    def __init__(self, precision=6):
        pass

    def normalise(self, value, default="", issues=None):
        if value == "":
            return default

        geometry = shapely.wkt.loads(value)

        if geometry.geom_type == "Point":
            first_point = geometry.coords[0]
        else:
            first_point = geometry.geoms[0].exterior.coords[0]

        lon, lat = first_point[:2]
        transposed_coords = False

        if degrees_like(lon, lat):
            if not within_england(lon, lat):
                lon, lat = lat, lon
                if within_england(lon, lat):
                    transposed_coords = True
                else:
                    if issues:
                        issues.log("WGS84 outside England", f"{value[0:30]}...")
                    return default
        elif easting_northing_like(lon, lat):
            lat, lon = osgb_to_wgs84.transform(lon, lat)
            if within_england(lon, lat):
                transposed_coords = True
            else:
                lat, lon = osgb_to_wgs84.transform(lat, lon)
                if not within_england(lon, lat):
                    if issues:
                        issues.log("OSGB outside England", f"{value[0:30]}...")
                    return default
            geometry = transform(osgb_to_wgs84.transform, geometry)
        elif osm_like(lon, lat):
            lat, lon = osm_to_wgs84.transform(lon, lat)
            if within_england(lon, lat):
                transposed_coords = True
            else:
                lat, lon = osm_to_wgs84.transform(lat, lon)
                if not within_england(lon, lat):
                    if issues:
                        issues.log("OSM outside England", f"{value[0:30]}...")
                    return default
            geometry = transform(osm_to_wgs84.transform, geometry)
        else:
            if issues:
                issues.log("out of range", f"{value[0:30]}...")
            return default

        if transposed_coords:
            geometry = transform(flip, geometry)

        if geometry.geom_type != "Point":
            geometry = geometry.simplify(0.00002)
            if not isinstance(geometry, MultiPolygon):
                # simplify will reduce to simple Polygon if possible
                geometry = MultiPolygon([geometry])

        return shapely.wkt.dumps(geometry, rounding_precision=6, output_dimension=2).replace(", ", ",")
