import shapely.wkt
from shapely import set_precision
import json
import logging
from shapely.geometry import shape, Point
from shapely.errors import WKTReadingError
from shapely.ops import transform
from shapely.geometry import MultiPolygon
from shapely.geometry.polygon import orient
from shapely.validation import explain_validity, make_valid
from pyproj import Transformer
from pyproj.transformer import TransformerGroup
from .datatype import DataType

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

DEFAULT_BOUNDARY = shapely.wkt.loads(
    "POLYGON ((2.95 56, 2.95 49.5, -7 49.5, -7 53.4, -4 53.4, -4 56, 2.95 56))"
)


def degrees_like(x, y):
    return x > -60.0 and x < 60.0 and y > -60.0 and y < 60.0


def easting_northing_like(x, y):
    return x > 1000.0 and x < 1000000.0 and y > 1000.0 and y < 1000000.0


def metres_like(x, y):
    return y > 6000000.0 and y < 10000000.0


def flip(x, y, z=None):
    return tuple(filter(None, [y, x, z]))


def parse_wkt(value, boundary):
    try:
        geometry = shapely.wkt.loads(value)
    except WKTReadingError:
        try:
            geometry = shapely.wkt.loads(shape(json.loads(value)).wkt)
            return geometry, "invalid type geojson", None
        except Exception:
            return (
                None,
                "invalid WKT",
                "Geometry must be in Well-Known Text (WKT) format",
            )

    if geometry.geom_type in ["Point", "LineString"]:
        first_point = geometry.coords[0]
    elif geometry.geom_type in ["Polygon"]:
        first_point = geometry.exterior.coords[0]
    elif geometry.geom_type in ["MultiPolygon"]:
        first_point = geometry.geoms[0].exterior.coords[0]
    elif geometry.geom_type in ["MultiLineString"]:
        first_point = geometry.geoms[0].coords[0]
    elif geometry.geom_type in ["GeometryCollection"]:
        first_geometry = geometry.geoms[0]
        if first_geometry.geom_type in ["MultiPolygon"]:
            first_point = first_geometry.geoms[0].exterior.coords[0]
        else:
            return (
                None,
                "Unexpected geom type within GeometryCollection",
                "Geometry must be a polygon",
            )
    else:
        return None, "Unexpected geom type", "Geometry must be a point or polygon"

    x, y = first_point[:2]
    boundary_issue_info = (
        "England" if (boundary == DEFAULT_BOUNDARY) else "custom boundary"
    )

    if degrees_like(x, y):
        if boundary.intersects(Point(x, y)):
            return geometry, None, None

        if boundary.intersects(Point(y, x)):
            return transform(flip, geometry), "WGS84 flipped", None

        return None, "WGS84 out of bounds of " + boundary_issue_info, None

    if easting_northing_like(x, y):
        _x, _y = osgb_to_wgs84.transform(x, y)
        if boundary.intersects(Point(_x, _y)):
            return transform(osgb_to_wgs84.transform, geometry), "OSGB", None
        _x, _y = osgb_to_wgs84.transform(y, x)
        if boundary.intersects(Point(_x, _y)):
            geometry = transform(flip, geometry)
            geometry = transform(osgb_to_wgs84.transform, geometry)
            return geometry, "OSGB flipped", None

        return None, "OSGB out of bounds of " + boundary_issue_info, None

    if metres_like(x, y):
        _x, _y = mercator_to_wgs84.transform(x, y)
        if boundary.intersects(Point(_x, _y)):
            return transform(mercator_to_wgs84.transform, geometry), "Mercator", None

    if metres_like(y, x):
        _x, _y = mercator_to_wgs84.transform(y, x)
        if boundary.intersects(Point(_x, _y)):
            geometry = transform(flip, geometry)
            geometry = transform(mercator_to_wgs84.transform, geometry)
            return geometry, "Mercator flipped", None

    return (
        None,
        "invalid coordinates",
        "Geometry must use WGS84, OSGB or Mercator coordinates",
    )


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
            elif geom.geom_type == "GeometryCollection":
                temp_polygons = make_multipolygon(geom)
                polygons.extend(temp_polygons.geoms)
            else:
                logging.info(f"skipping {geom.geom_type}")
        return MultiPolygon(polygons)

    raise ValueError(f"unexpected geometry {geometry.geom_type}")


def normalise_geometry(geometry, simplification=0.000005):
    if geometry.geom_type in ["Point", "Line", "MultiLineString"]:
        return geometry, None

    # see https://gist.github.com/psd/0189bc66fd46e00a82df2acbc7e35c8a
    # don't want to simplify if it takes a valid shape and makes it invalid
    simplification = geometry.simplify(simplification)
    if not geometry.is_valid or simplification.is_valid:
        geometry = simplification

    geometry = set_precision(geometry, 0.000001, mode="pointwise")

    # check and resolve an invalid geometry
    # result may be a GeometryCollection containing points and lines
    # https://shapely.readthedocs.io/en/stable/manual.html#validation.make_valid
    issue = None
    if not geometry.is_valid:
        issue = explain_validity(geometry)
        geometry = make_valid(geometry)

    # ensure geometry is a MultiPolygon
    geometry = make_multipolygon(geometry)

    # uses a buffer to combine overlapping polyongs inside the multipolygon
    # this is very common when simplifying a geometry collection as it's
    # usually why it's a geometry collection not a multipolygon
    # ToDO should this be in the make_multipolygon function? Should it record an error?
    if geometry:
        if not geometry.is_valid:
            geometry = geometry.buffer(0)

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

    def normalise(self, value, default="", issues=None, boundary=None):
        if not value:
            return default

        if boundary:
            try:
                boundary_wkt = shapely.wkt.loads(boundary)
                if boundary_wkt.geom_type in ["Polygon", "MultiPolygon"]:
                    boundary = boundary_wkt
                else:
                    issues.log(
                        "Invalid boundary provided - boundary must be of type Polygon or MultiPolygon",
                        "",
                    )
                    boundary = DEFAULT_BOUNDARY
            except WKTReadingError:
                issues.log("Error reading boundary - must be a WKT", "")
                boundary = DEFAULT_BOUNDARY
        else:
            boundary = DEFAULT_BOUNDARY

        geometry, issue, message = parse_wkt(value, boundary)

        if issues and issue:
            issues.log(issue, "", message=message)

        if geometry:
            # Reduce precision prior to normalisation.
            # this prevents reintroduction of errors fixed by
            # normalisation process. This was happening in some
            # cases in the final dump_wkt call on more precise,
            # fixed/normalised geometry. To reduce precision,
            # round trip the geometry through shapely with 6 dp precision.

            _wkt = dump_wkt(geometry)
            geometry = shapely.wkt.loads(_wkt)

            geometry, issue = normalise_geometry(geometry)

            if geometry:
                if geometry.is_valid:
                    # if the geometry is valid at this point log any issue that has been fixed
                    if issues and issue:
                        issues.log("invalid geometry - fixed", issue)
                else:
                    # if the geometry is not valid, mark as not fixable
                    if issues and issue:
                        issues.log("invalid geometry - not fixable", issue)

        if not geometry:
            return default

        return dump_wkt(geometry)
