from .wkt import WktDataType


class MultiPolygon(WktDataType):
    def normalise(self, values, issues=None):
        return super().normalise(values, issues=issues)
