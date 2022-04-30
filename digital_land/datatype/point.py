from .wkt import WktDataType, parse_wkt


class PointDataType(WktDataType):
    def normalise(self, values, default=["", ""], issues=None):
        if not values or "" in values:
            return default

        point = f"POINT ({values[0]} {values[1]})"
        point = super().normalise(point, issues=issues)

        if not point:
            return default

        geometry, issue = parse_wkt(point)
        x, y = geometry.coords[0]

        return [str(x), str(y)]
