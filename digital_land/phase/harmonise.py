import re
from datetime import datetime

from .phase import Phase
from digital_land.datatype.point import PointDataType


class HarmonisePhase(Phase):
    patch = {}

    def __init__(
        self,
        specification=None,
        issues=None,
        patches={},
        plugin_manager=None,
    ):
        self.specification = specification
        self.issues = issues
        self.patch = patches

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        self.issues.fieldname = fieldname
        datatype = self.specification.field_type(fieldname)
        return datatype.normalise(value, issues=self.issues)

    def apply_patch(self, fieldname, value):
        patches = {**self.patch.get(fieldname, {}), **self.patch.get("", {})}
        for pattern, replacement in patches.items():
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if match:
                newvalue = match.expand(replacement)
                if newvalue != value:
                    self.issues.log_issue(fieldname, "patch", value)
                return newvalue

        return value

    def process(self, stream):
        for block in stream:
            row = block["row"]

            self.issues.dataset = block["dataset"]
            self.issues.resource = block["resource"]
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            o = {}

            for field in row:
                row[field] = self.apply_patch(field, row[field])
                o[field] = self.harmonise_field(field, row[field])

            # future entry dates
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
                    o[typology] = "%s:%s" % (row["dataset"], value)

            # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs with a wikipedia-en prefix
            if row.get("wikipedia", "").startswith("http"):
                self.issues.log_issue("wikipedia", "removed prefix", row["wikipedia"])
                o["wikipedia"] = row["wikipedia"].replace(
                    "https://en.wikipedia.org/wiki/", ""
                )

            block["row"] = o
            yield block
