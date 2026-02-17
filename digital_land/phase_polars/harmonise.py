import logging
from datetime import datetime, date
from calendar import monthrange

import polars as pl

from .phase import PolarsPhase
from digital_land.datatype.point import PointDataType
from digital_land.datatype.factory import datatype_factory

try:
    import shapely.wkt
except ImportError:
    shapely = None

logger = logging.getLogger(__name__)

MANDATORY_FIELDS_DICT = {
    "article-4-direction": [
        "reference", "name", "document-url", "documentation-url",
    ],
    "article-4-direction-area": [
        "reference", "geometry", "name", "permitted-development-rights",
    ],
    "conservation-area": ["reference", "geometry", "name"],
    "conservation-area-document": [
        "reference", "name", "conservation-area",
        "document-url", "documentation-url", "document-type",
    ],
    "tree-preservation-order": [
        "reference", "document-url", "documentation-url",
    ],
    "tree-preservation-zone": ["reference", "geometry"],
    "listed-building-outline": ["reference", "geometry", "name", "listed-building"],
    "tree": ["reference", "point", "geometry"],
    "brownfield-land": [
        "OrganisationURI", "SiteReference", "SiteNameAddress", "GeoX", "GeoY",
    ],
}

FAR_FUTURE_YEARS_AHEAD = 50


class HarmonisePhase(PolarsPhase):
    """
    Harmonise field values according to their datatype specification.

    This phase delegates to the existing datatype normalisation logic on a
    per-row basis using map_elements for correctness, since individual
    datatype classes contain complex transformation rules.
    """

    def __init__(
        self,
        field_datatype_map=None,
        issues=None,
        dataset=None,
        valid_category_values=None,
    ):
        if field_datatype_map is None:
            field_datatype_map = {}
        if valid_category_values is None:
            valid_category_values = {}
        self.field_datatype_map = field_datatype_map
        self.issues = issues
        self.dataset = dataset
        self.valid_category_values = valid_category_values

    def _get_far_future_date(self, number_of_years_ahead: int):
        today = date.today()
        y = today.year + number_of_years_ahead
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)

    def _harmonise_row(self, row_dict, resource, line_number, entry_number):
        """Harmonise a single row – mirrors the streaming HarmonisePhase exactly."""
        if self.issues:
            self.issues.resource = resource
            self.issues.line_number = line_number
            self.issues.entry_number = entry_number

        o = {}
        for field, value in row_dict.items():
            if field.startswith("__"):
                continue

            # Category value validation
            if field in self.valid_category_values:
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
                        value = matching_value
                    else:
                        if self.issues:
                            self.issues.log_issue(
                                field, "invalid category value", value
                            )

            # Harmonise via datatype
            if not value:
                o[field] = ""
            elif field in self.field_datatype_map:
                if self.issues:
                    self.issues.fieldname = field
                datatype_name = self.field_datatype_map[field]
                if datatype_name == "datetime":
                    far_past_date = date(1799, 12, 31)
                    far_future_date = self._get_far_future_date(FAR_FUTURE_YEARS_AHEAD)
                    datatype = datatype_factory(
                        datatype_name=datatype_name,
                        far_past_date=far_past_date,
                        far_future_date=far_future_date,
                    )
                else:
                    datatype = datatype_factory(datatype_name=datatype_name)
                o[field] = datatype.normalise(value, issues=self.issues)
            else:
                o[field] = value

        # Future entry-date check
        for field in ["entry-date", "LastUpdatedDate"]:
            val = o.get(field, "")
            if val:
                try:
                    if datetime.strptime(val[:10], "%Y-%m-%d").date() > datetime.today().date():
                        if self.issues:
                            self.issues.log_issue(
                                field, "future entry-date", row_dict.get(field, ""),
                                f"{field} must be today or in the past",
                            )
                        o[field] = ""
                except (ValueError, TypeError):
                    pass

        # GeoX/GeoY handling
        if "GeoX" in row_dict and "GeoY" in row_dict:
            if self.issues:
                self.issues.fieldname = "GeoX,GeoY"
            point = PointDataType()
            try:
                geometry = point.normalise(
                    [o.get("GeoX", ""), o.get("GeoY", "")],
                    issues=self.issues,
                )
                if geometry and shapely:
                    point_geometry = shapely.wkt.loads(geometry)
                    x, y = point_geometry.coords[0]
                    o["GeoX"] = str(x)
                    o["GeoY"] = str(y)
                elif not geometry:
                    o.pop("GeoX", None)
                    o.pop("GeoY", None)
            except Exception as e:
                logger.error(
                    f"Exception occurred while fetching geoX, geoY coordinates: {e}"
                )

        # Typology prefix
        for typology in ["organisation", "geography", "document"]:
            value = o.get(typology, "")
            if value and ":" not in value:
                o[typology] = f"{self.dataset}:{value}"

        # Mandatory field checks
        mandatory_fields = MANDATORY_FIELDS_DICT.get(self.dataset)
        for field in row_dict:
            if field.startswith("__"):
                continue
            if field in ["geometry", "point"]:
                if not row_dict.get("geometry") and not row_dict.get("point"):
                    if self.issues:
                        self.issues.log_issue(
                            field, "missing value", "", f"{field} missing"
                        )
            elif mandatory_fields and field in mandatory_fields:
                if not row_dict.get(field):
                    if self.issues:
                        self.issues.log_issue(
                            field, "missing value", "", f"{field} missing"
                        )

        # Wikipedia
        if row_dict.get("wikipedia", "").startswith("http"):
            if self.issues:
                self.issues.log_issue(
                    "wikipedia", "removed URI prefix", row_dict["wikipedia"]
                )
            o["wikipedia"] = row_dict["wikipedia"].replace(
                "https://en.wikipedia.org/wiki/", ""
            )

        return o

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        meta_cols = [c for c in df.columns if c.startswith("__")]
        data_cols = [c for c in df.columns if not c.startswith("__")]

        results = []
        for row in df.iter_rows(named=True):
            resource = row.get("__resource", "")
            line_number = row.get("__line_number", 0)
            entry_number = row.get("__entry_number", 0)

            harmonised = self._harmonise_row(row, resource, line_number, entry_number)

            # Include metadata
            out = {}
            for mc in meta_cols:
                out[mc] = row[mc]
            for field in data_cols:
                out[field] = harmonised.get(field, "")
            results.append(out)

        if not results:
            return df.clear()

        return pl.DataFrame(results, schema={c: pl.Utf8 for c in results[0] if not c.startswith("__")} | {c: df.schema[c] for c in meta_cols if c in df.schema})
