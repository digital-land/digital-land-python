import shapely.wkt
from shapely.geometry import MultiPolygon, Polygon, GeometryCollection
from .wkt import WktDataType


class MultiPolygonDataType(WktDataType):
    def normalise(self, values, default=None, issues=None):
        try:
            # Try to load the value as WKT
            multipolygon = shapely.wkt.loads(values)

            # Check if it's a MultiPolygon
            if not isinstance(
                multipolygon, (Polygon, MultiPolygon, GeometryCollection)
            ):
                issues.log(
                    "Unexpected geom type", values, "Geometry must be a multipolygon"
                )
                return default

        except shapely.errors.WKTReadingError:
            # If loading as WKT fails, log an error
            issues.log(
                "invalid WKT",
                values,
                "Geometry must be in Well-Known Text (WKT) format",
            )
            return default

        # Normalize the multipolygon representation
        normalized_multipolygon = super().normalise(multipolygon, issues=issues)

        if not normalized_multipolygon:
            return default

        return normalized_multipolygon
