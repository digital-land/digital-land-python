import shapely.wkt
from .wkt import WktDataType
from shapely.geometry import Point


class PointDataType(WktDataType):
    def normalise(self, values, default=["", ""], issues=None):
        try:
            # Try to load the value as WKT
            point = shapely.wkt.loads(values)
            if not isinstance(point, Point):
                issues.log("Unexpected geom type", values, "Geometry must be a point")
                return ""
        except shapely.errors.WKTReadingError:
            # If loading as WKT fails, assume it's a pair of coordinates
            try:
                point = Point(float(values[0]), float(values[1]))
            except (ValueError, IndexError):
                return default

        # Normalize the point representation
        point = super().normalise(point, issues=issues)
        if not point:
            return default
        return point
