from .phase import Phase
from digital_land.datatype.point import PointDataType


class PointPhase(Phase):
    """
    add a point field from latitude/longitude fields
    """

    def __init__(self, issues=None):
        self.issues = issues

    def process(self, stream):
        for block in stream:
            row = block["row"]

            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                (row["GeoX"], row["GeoY"]) = point.normalise(
                    [row["GeoX"], row["GeoY"]], issues=self.issues
                )

            yield block
