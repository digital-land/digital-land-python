import shapely.wkt
from .wkt import WktDataType


class PointDataType(WktDataType):
    def normalise(self, values, default=["", ""], issues=None, boundary=None):
        if not values or "" in values:
            return default

        point = f"POINT ({values[0]} {values[1]})"
        point = super().normalise(point, issues=issues, boundary=boundary)

        if not point:
            return default

        geometry = shapely.wkt.loads(point)
        x, y = geometry.coords[0]

        return [str(x), str(y)]
