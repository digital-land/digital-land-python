from datetime import datetime, date
from .phase import Phase
from calendar import monthrange
from digital_land.datatype.point import PointDataType
from digital_land.datatype.factory import datatype_factory
import shapely.wkt
import logging
from digital_land.utils.timer import timer

logger = logging.getLogger(__name__)

# Storing mandatory fields in dict for now until added to specification
MANDATORY_FIELDS_DICT = {
    "article-4-direction": [
        "reference",
        "name",
        "document-url",
        "documentation-url",
    ],
    "article-4-direction-area": [
        "reference",
        "geometry",
        "name",
        "permitted-development-rights",
    ],
    "conservation-area": ["reference", "geometry", "name"],
    "conservation-area-document": [
        "reference",
        "name",
        "conservation-area",
        "document-url",
        "documentation-url",
        "document-type",
    ],
    "tree-preservation-order": [
        "reference",
        "document-url",
        "documentation-url",
    ],
    "tree-preservation-zone": ["reference", "geometry"],
    "listed-building-outline": ["reference", "geometry", "name", "listed-building"],
    "tree": ["reference", "point", "geometry"],
    "brownfield-land": [
        "OrganisationURI",
        "SiteReference",
        "SiteNameAddress",
        "GeoX",
        "GeoY",
    ],
    "developer-agreement": [
        "reference",
    ],
    "developer-agreement-contribution": [
        "reference",
    ],
    "developer-agreement-transaction": [
        "reference",
    ],
    "infrastructure-funding-statement": [
        "reference",
    ],
}

FAR_FUTURE_YEARS_AHEAD = 50


class HarmonisePhase(Phase):
    def __init__(
        self,
        field_datatype_map,
        issues=None,
        dataset=None,
        valid_category_values={},  # { field: list of valid values }
    ):
        self.field_datatype_map = field_datatype_map
        self.issues = issues
        self.dataset = dataset
        self.valid_category_values = valid_category_values

    def get_field_datatype_name(self, fieldname):
        try:
            return self.field_datatype_map[fieldname]
        except KeyError:
            raise ValueError(f"field {fieldname} does not have a datatype mapping")

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        self.issues.fieldname = fieldname
        datatype_name = self.get_field_datatype_name(fieldname)
        if datatype_name == "datetime":
            # add defaullt configuration for datetime
            far_past_date = date(1799, 12, 31)
            far_future_date = self._get_far_future_date(FAR_FUTURE_YEARS_AHEAD)
            datatype = datatype_factory(
                datatype_name=datatype_name,
                far_past_date=far_past_date,
                far_future_date=far_future_date,
            )
        # for datetimes add default far past and far future issue types
        else:
            datatype = datatype_factory(datatype_name=datatype_name)

        return datatype.normalise(value, issues=self.issues)

    @timer
    def process(self, stream):
        for block in stream:
            row = block["row"]
            self.issues.resource = block["resource"]
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            o = {}

            # Categorical field check
            for field in row:
                if field in self.valid_category_values.keys():
                    value = row[field]
                    if value:
                        normalised_value = value.replace(" ", "-")
                        matching_value = next(
                            (
                                v
                                for v in self.valid_category_values[field]
                                if v.lower() == normalised_value.lower()
                            ),
                            None,
                        )
                        if matching_value:
                            # use exact value from self.valid_category_values
                            # TODO: log a warning where we've replaced spaces to match categorical value
                            row[field] = matching_value
                        else:
                            self.issues.log_issue(
                                field, "invalid category value", value
                            )
                o[field] = self.harmonise_field(field, row[field])

            # remove future entry dates
            for field in ["entry-date", "LastUpdatedDate"]:
                if (
                    o.get(field, "")
                    and datetime.strptime(o[field][:10], "%Y-%m-%d").date()
                    > datetime.today().date()
                ):
                    self.issues.log_issue(
                        field,
                        "future entry-date",
                        row[field],
                        f"{field} must be today or in the past",
                    )
                    o[field] = ""

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                try:
                    geometry = point.normalise(
                        [o["GeoX"], o["GeoY"]], issues=self.issues
                    )
                    if geometry:
                        point_geometry = shapely.wkt.loads(geometry)
                        x, y = point_geometry.coords[0]
                        (o["GeoX"], o["GeoY"]) = [str(x), str(y)]
                    else:
                        # Remove the invalid point
                        del o["GeoX"]
                        del o["GeoY"]
                except Exception as e:
                    logger.error(
                        f"Exception occurred while fetching geoX, geoY coordinates: {e}"
                    )
            # TODO need to identify why below exists and possibly remove
            # ensure typology fields are a CURIE
            for typology in ["organisation", "geography", "document"]:
                value = o.get(typology, "")
                if value and ":" not in value:
                    o[typology] = "%s:%s" % (self.dataset, value)

            mandatory_fields = MANDATORY_FIELDS_DICT.get(self.dataset)

            # Check for missing values in mandatory fields
            # Only checking fields given to us - not checking for missing fields
            # One of geometry or point must not be empty if either field is given
            for field in row:
                if field in ["geometry", "point"]:
                    if (
                        row.get("geometry") == "" or row.get("geometry") is None
                    ) and (row.get("point") == "" or row.get("point") is None):
                        self.issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )
                elif mandatory_fields and field in mandatory_fields:
                    if row.get(field) == "" or row.get(field) is None:
                        self.issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )

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

    def _get_far_future_date(self, number_of_years_ahead: int):
        today = date.today()
        y = today.year + number_of_years_ahead
        # keep same month/day if possible (handles Feb 29 & short months)
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)
