import shapely.wkt
import json
from shapely.geometry import MultiPolygon, Polygon, GeometryCollection, shape
from .wkt import WktDataType


class MultiPolygonDataType(WktDataType):
    def normalise(self, values, default="", issues=None, boundary=None):
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
            # If loading as WKT fails, try if it's in a json format
            try:
                geometry = shapely.wkt.loads(shape(json.loads(values)).wkt)
                if not isinstance(
                    geometry, (Polygon, MultiPolygon, GeometryCollection)
                ):
                    issues.log(
                        "Unexpected geom type",
                        values,
                        "Geometry must be a multipolygon",
                    )
                multipolygon = values
            except Exception:
                issues.log(
                    "invalid WKT",
                    values,
                    "Geometry must be in Well-Known Text (WKT) format",
                )
                return default

        # Normalize the multipolygon representation
        print("b:", boundary)
        normalized_multipolygon = super().normalise(
            multipolygon, issues=issues, boundary=boundary
        )

        if not normalized_multipolygon:
            return default

        return normalized_multipolygon
