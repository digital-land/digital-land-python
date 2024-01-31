from datetime import datetime

from .phase import Phase
from digital_land.datatype.point import PointDataType


class HarmonisePhase(Phase):
    def __init__(
        self,
        specification=None,
        issues=None,
    ):
        self.specification = specification
        self.issues = issues

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        self.issues.fieldname = fieldname
        datatype = self.specification.field_type(fieldname)
        return datatype.normalise(value, issues=self.issues)

    def process(self, stream):
        for block in stream:
            row = block["row"]
            self.issues.resource = block["resource"]
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            o = {}

            for field in row:
                o[field] = self.harmonise_field(field, row[field])

            # remove future entry dates
            for field in ["entry-date", "LastUpdatedDate"]:
                if (
                    o.get(field, "")
                    and datetime.strptime(o[field][:10], "%Y-%m-%d").date()
                    > datetime.today().date()
                ):
                    self.issues.log_issue(field, "future entry-date", row[field])
                    o[field] = ""

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                (o["GeoX"], o["GeoY"]) = point.normalise(
                    [o["GeoX"], o["GeoY"]], issues=self.issues
                )

            # ensure typology fields are a CURIE
            for typology in ["organisation", "geography", "document"]:
                value = o.get(typology, "")
                if value and ":" not in value:
                    o[typology] = "%s:%s" % (block["dataset"], value)

            # ensure geometry field is not empty
            for typology in ["geography"]:
                # logging error when both geometry & point are empty
                # TO-DO: will replace this code once we get mandatory list from Specification
                for field in row:
                    if field in ["geometry", "point"]:
                        if (
                            row.get("geometry") == "" or row.get("geometry") is None
                        ) and (row.get("point") == "" or row.get("point") is None):
                            self.issues.log_issue(field, "missing value", "")

            # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs with a wikipedia-en prefix
            if row.get("wikipedia", "").startswith("http"):
                self.issues.log_issue(
                    "wikipedia", "removed URI prefix", row["wikipedia"]
                )
                o["wikipedia"] = row["wikipedia"].replace(
                    "https://en.wikipedia.org/wiki/", ""
                )
            block["row"] = o

            yield block
