from .wkt import WktDataType


class MultiPolygon(WktDataType):
    def normalise(self, values, issues=None):
        super().normalise(values, issues=issues)
