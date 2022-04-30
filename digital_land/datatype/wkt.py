import shapely.wkt
from shapely.ops import transform
from shapely.geometry import MultiPolygon
from pyproj import Transformer
from .datatype import DataType


# convert from OSGB Northings and Eastings to WGS84
# https://epsg.io/27700
# https://epsg.io/4326
osgb_to_wgs84 = Transformer.from_crs(27700, 4326)

# convert from Pseudo-Mercator metres to WGS84 decimal degrees
# https://epsg.io/3857
# https://epsg.io/4326
# https://pyproj4.github.io/pyproj/stable/api/transformer.html#transformer
mercator_to_wgs84 = Transformer.from_crs(3857, 4326, always_xy=True)


def degrees_like(x, y):
    return x > -60.0 and x < 60.0 and y > -60.0 and y < 60.0


def easting_northing_like(x, y):
    return x > 1000.0 and x < 1000000.0 and y > 1000.0 and y < 1000000.0


def metres_like(x, y):
    return y > 6000000.0 and y < 10000000.0


# bounding box check
def within_england(x, y):
    return x > -7.0 and x < 2 and y > 49.5 and y < 56.0


# check if NW of the SE corner of the Irish Sea
def osgb_within_england(x, y):
    return not (x < 270000.0 and y > 400000.0)


def flip(x, y, z=None):
    return tuple(filter(None, [y, x, z]))


def parse_wkt(value):
    # TBD: turn exception into issue
    geometry = shapely.wkt.loads(value)

    if geometry.geom_type == "Point":
        first_point = geometry.coords[0]
    else:
        first_point = geometry.geoms[0].exterior.coords[0]

    x, y = first_point[:2]

    if degrees_like(x, y):
        if within_england(x, y):
            return geometry, None

        if within_england(y, x):
            return transform(flip, geometry), "WGS84 flipped"

        return None, "WGS84 out of bounds"

    if easting_northing_like(x, y):
        if osgb_within_england(x, y):
            return transform(osgb_to_wgs84.transform, geometry), "OSGB"

        if osgb_within_england(y, x):
            geometry = transform(flip, geometry)
            geometry = transform(osgb_to_wgs84.transform, geometry)
            geometry = transform(flip, geometry)
            return geometry, "OSGB flipped"

        return None, "OSGB out of bounds"

    if metres_like(x, y):
        _x, _y = mercator_to_wgs84.transform(x, y)
        if within_england(_x, _y):
            return transform(mercator_to_wgs84.transform, geometry), "Mercator"

    if metres_like(y, x):
        _x, _y = mercator_to_wgs84.transform(y, x)
        if within_england(_x, _y):
            geometry = transform(flip, geometry)
            geometry = transform(mercator_to_wgs84.transform, geometry)
            return geometry, "Mercator flipped"

    return None, "invalid"


def dump_wkt(geometry, precision=6, simplification=0.000005, dimensions=2):
    if geometry.geom_type != "Point":

        # see https://gist.github.com/psd/0189bc66fd46e00a82df2acbc7e35c8a
        geometry = geometry.simplify(simplification)

        # force geometry to be MULTIPOLYGON
        if not isinstance(geometry, MultiPolygon):
            geometry = MultiPolygon([geometry])

    wkt = shapely.wkt.dumps(
        geometry, rounding_precision=precision, output_dimension=dimensions
    )
    return wkt.replace(", ", ",")


class WktDataType(DataType):
    def __init__(self):
        pass

    def normalise(self, value, default="", issues=None):
        if not value:
            return default

        geometry, issue = parse_wkt(value)

        if issue:
            issues.log(issue, "")

        if not geometry:
            return default

        return dump_wkt(geometry)
