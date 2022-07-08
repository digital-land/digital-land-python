import shapely.wkt
from shapely.errors import WKTReadingError
from shapely.ops import transform
from shapely.geometry import MultiPolygon
from shapely.geometry.polygon import orient
from shapely.validation import explain_validity, make_valid
from pyproj import Transformer
from pyproj.transformer import TransformerGroup
from .datatype import DataType
import logging


# use PyProj to transform coordinates between systems
# https://pyproj4.github.io/pyproj/stable/api/transformer.html#transformer

if not TransformerGroup("epsg:27700", "epsg:4326").best_available:
    logging.warning("not using the best available OSGB correction tables")

# convert from OSGB Northings and Eastings to WGS84
# https://epsg.io/27700
# https://epsg.io/4326
osgb_to_wgs84 = Transformer.from_crs(27700, 4326, always_xy=True)

# convert from Pseudo-Mercator metres to WGS84 decimal degrees
# https://epsg.io/3857
# https://epsg.io/4326
mercator_to_wgs84 = Transformer.from_crs(3857, 4326, always_xy=True)


def degrees_like(x, y):
    return x > -60.0 and x < 60.0 and y > -60.0 and y < 60.0


def easting_northing_like(x, y):
    return x > 1000.0 and x < 1000000.0 and y > 1000.0 and y < 1000000.0


def metres_like(x, y):
    return y > 6000000.0 and y < 10000000.0


# bounding box check
def within_england(x, y):
    return x > -7.0 and x < 2.5 and y > 49.5 and y < 56.0


# check if NW of the SE corner of the Irish Sea
# https://gridreferencefinder.com/?gr=SC7000000000
def osgb_within_england(x, y):
    return not (x < 270000.0 and y > 400000.0)


def flip(x, y, z=None):
    return tuple(filter(None, [y, x, z]))


def parse_wkt(value):
    try:
        geometry = shapely.wkt.loads(value)
    except WKTReadingError:
        return None, "invalid WKT"

    if geometry.geom_type in ["Point", "LineString"]:
        first_point = geometry.coords[0]
    elif geometry.geom_type in ["Polygon"]:
        first_point = geometry.exterior.coords[0]
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

    return None, "invalid coordinates"


def make_multipolygon(geometry):
    if geometry.geom_type in ["Point", "Line", "LineString", "MultiLineString"]:
        return None

    if geometry.geom_type == "MultiPolygon":
        return geometry

    if geometry.geom_type == "Polygon":
        return MultiPolygon([geometry])

    if geometry.geom_type == "GeometryCollection":
        polygons = []
        for geom in geometry.geoms:
            if geom.geom_type == "Polygon":
                polygons.append(geom)
            elif geom.geom_type == "MultiPolygon":
                for polygon in geom.geoms:
                    polygons.append(polygon)
            else:
                logging.info(f"skipping {geom.geom_type}")
        return MultiPolygon(polygons)

    raise ValueError(f"unexpected geometry {geometry.geom_type}")


def normalise_geometry(geometry, simplification=0.000005):
    if geometry.geom_type in ["Point", "Line", "MultiLineString"]:
        return geometry, None

    # see https://gist.github.com/psd/0189bc66fd46e00a82df2acbc7e35c8a
    geometry = geometry.simplify(simplification)

    # check and resolve an invalid geometry
    # result may be a GeometryCollection containing points and lines
    # https://shapely.readthedocs.io/en/stable/manual.html#validation.make_valid
    issue = None
    if not geometry.is_valid:
        issue = explain_validity(geometry)
        geometry = make_valid(geometry)

    # ensure geometry is a MultiPolygon
    geometry = make_multipolygon(geometry)

    # fix winding order
    # WKT external rings should be counterclockwise, interior rings clockwise
    # https://shapely.readthedocs.io/en/stable/manual.html#shapely.geometry.polygon.orient
    if geometry:
        polygons = []
        for geom in geometry.geoms:
            polygons.append(orient(geom))
        geometry = MultiPolygon(polygons)

    return geometry, issue


def dump_wkt(geometry, precision=6, dimensions=2):
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

        if geometry:

            # Reduce precision prior to normalisation.
            # this prevents reintroduction of errors fixed by
            # normalisation process. This was happening in some
            # cases in the final dump_wkt call on more precise,
            # fixed/normalised geometry. To reduce precision,
            # round trip the geometry through shapely with 6 dp precision.

            _wkt = dump_wkt(geometry, precision=6)
            geometry = shapely.wkt.loads(_wkt)

            geometry, issue = normalise_geometry(geometry)

            if issue:
                issues.log("invalid geometry", issue)

        if not geometry:
            return default

        return dump_wkt(geometry)
