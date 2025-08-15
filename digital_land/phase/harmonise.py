from datetime import datetime
from .phase import Phase
from digital_land.datatype.point import PointDataType
from digital_land.datatype.factory import datatype_factory
import shapely.wkt
import logging

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
}

# Datasets exempt from the global "reference" requirement
EXEMPT_REFERENCE_DATASETS = {"brownfield-land"}


class HarmonisePhase(Phase):
    def __init__(
        self,
        field_datatype_map,
        issues=None,
        dataset=None,
        valid_category_values=None,  # { field: list of valid values }
    ):
        self.field_datatype_map = field_datatype_map
        self.issues = issues
        self.dataset = dataset
        self.valid_category_values = valid_category_values or {}

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
        datatype = datatype_factory(datatype_name=datatype_name)
        return datatype.normalise(value, issues=self.issues)

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

            # ensure typology fields are a CURIE
            for typology in ["organisation", "geography", "document"]:
                value = o.get(typology, "")
                if value and ":" not in value:
                    o[typology] = "%s:%s" % (self.dataset, value)

            # ------------------------------------------------------------------
            # GLOBAL: enforce "reference" exists and is non-empty for ALL datasets,
            # except those explicitly exempted (e.g., "brownfield-land").
            # This runs before any dataset-specific mandatory checks.
            # ------------------------------------------------------------------
            if self.dataset not in EXEMPT_REFERENCE_DATASETS:
                if ("reference" not in row) or (row.get("reference") in ("", None)):
                    self.issues.log_issue(
                        "reference",
                        "missing value",
                        "",
                        "reference missing",
                    )

            # Determine which fields are mandatory for this dataset
            is_known_dataset = self.dataset in MANDATORY_FIELDS_DICT
            mandatory_fields = MANDATORY_FIELDS_DICT.get(self.dataset, ["reference"])

            # For known datasets, avoid double-logging "reference" (global check above)
            mandatory_fields_excl_reference = [
                f for f in mandatory_fields if f != "reference"
            ]

            # Missing value checks
            if is_known_dataset:
                # One of geometry or point must not be empty if either field is given
                for field in row:
                    if field in ["geometry", "point"]:
                        if (row.get("geometry") in ("", None)) and (
                            row.get("point") in ("", None)
                        ):
                            self.issues.log_issue(
                                field,
                                "missing value",
                                "",
                                f"{field} missing",
                            )
                    elif field in mandatory_fields_excl_reference:
                        if row.get(field) in ("", None):
                            self.issues.log_issue(
                                field,
                                "missing value",
                                "",
                                f"{field} missing",
                            )
            else:
                # Unknown datasets: nothing further; global reference check above is sufficient.
                pass

            # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs
            # with a wikipedia-en prefix
            if row.get("wikipedia", "").startswith("http"):
                self.issues.log_issue(
                    "wikipedia", "removed URI prefix", row["wikipedia"]
                )
                o["wikipedia"] = row["wikipedia"].replace(
                    "https://en.wikipedia.org/wiki/", ""
                )
            block["row"] = o

            yield block
