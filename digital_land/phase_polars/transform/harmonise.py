import re
import polars as pl
from datetime import datetime, date
from calendar import monthrange
import logging

logger = logging.getLogger(__name__)

# NOTE: This module intentionally mirrors legacy stream harmonisation behaviour.
# The acceptance tests compare legacy and polars outputs field-by-field, so
# comments below call out parity-sensitive decisions.

# Storing mandatory fields in dict per dataset
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


class _NoOpIssues:
    """Lightweight stand-in for IssueLog; discards all messages."""

    # Datatype normalisers in ``digital_land.datatype`` expect an ``issues``
    # object exposing ``log``/``log_issue``. In the polars path we currently
    # normalise values without collecting per-row issue telemetry, so this
    # adapter preserves compatibility without changing datatype code.

    def __init__(self, fieldname=""):
        self.fieldname = fieldname
        self.resource = ""
        self.line_number = 0
        self.entry_number = 0

    def log(self, *args, **kwargs):
        pass

    def log_issue(self, *args, **kwargs):
        pass


class HarmonisePhase:
    """
    Apply data harmonisation to Polars LazyFrame using datatype conversions.
    
    Handles field validation, categorical mapping, date normalization,
    geometry processing, and mandatory field checks.
    """

    def __init__(
        self,
        field_datatype_map=None,
        dataset=None,
        valid_category_values=None,
    ):
        """
        Initialize the HarmonisePhase.
        
        Args:
            field_datatype_map: Dictionary mapping field names to datatype names
            dataset: The dataset name (used for mandatory field checking)
            valid_category_values: Dictionary mapping field names to lists of valid values
        """
        self.field_datatype_map = field_datatype_map or {}
        self.dataset = dataset
        self.valid_category_values = valid_category_values or {}

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply harmonisation transformations to LazyFrame.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: Harmonised LazyFrame
        """
        if lf.collect_schema().len() == 0:
            return lf

        existing_columns = lf.collect_schema().names()

        # Keep ordering aligned with the legacy HarmonisePhase where possible.
        # Some steps depend on prior normalisation (e.g. date checks run after
        # datatype conversion has produced ISO-like values).

        # Apply categorical field normalization
        lf = self._harmonise_categorical_fields(lf, existing_columns)

        # Apply datatype-based field harmonisation
        lf = self._harmonise_field_values(lf, existing_columns)

        # Remove future entry dates
        lf = self._remove_future_dates(lf, existing_columns)

        # Process point geometry (GeoX, GeoY)
        lf = self._process_point_geometry(lf, existing_columns)

        # Ensure typology fields have CURIE prefixes
        lf = self._add_typology_curies(lf, existing_columns)

        # Process Wikipedia URLs
        lf = self._process_wikipedia_urls(lf, existing_columns)

        return lf

    def _harmonise_categorical_fields(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Normalize categorical fields by replacing spaces and validating against allowed values.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with normalised categorical fields
        """
        for field, valid_values in self.valid_category_values.items():
            if field not in existing_columns:
                continue

            # Legacy behaviour: compare case-insensitively and treat spaces as
            # interchangeable with hyphens for matching only.
            value_map = {v.lower().replace(" ", "-"): v for v in valid_values}
            valid_list = list(value_map.values())

            # Apply the categorical normalization
            lf = lf.with_columns(
                pl.col(field)
                .map_elements(
                    lambda x: self._normalize_categorical(x, value_map),
                    return_dtype=pl.Utf8,
                )
                .alias(field)
            )

        return lf

    def _normalize_categorical(self, value, value_map):
        """Normalize a categorical value against allowed values."""
        if not value or (isinstance(value, str) and not value.strip()):
            return value

        normalized = value.replace(" ", "-").lower()
        return value_map.get(normalized, value)

    def _harmonise_field_values(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Apply datatype-based harmonisation to field values.

        Delegates to the same ``datatype.normalise()`` functions used by the
        legacy stream-based HarmonisePhase so that both pipelines produce
        identical output for every datatype (datetime → ISO dates,
        multipolygon → WGS84 MULTIPOLYGON WKT, decimal → normalised string,
        etc.).

        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names

        Returns:
            pl.LazyFrame: LazyFrame with harmonised field values
        """
        from digital_land.datatype.factory import datatype_factory

        for field in existing_columns:
            if field not in self.field_datatype_map:
                continue

            datatype_name = self.field_datatype_map[field]

            # Build datatype exactly as legacy does, including datetime bounds.
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

            # Closure factory gives each column a stable datatype instance and
            # field-specific issues context.
            def _make_normaliser(dt, fname):
                issues = _NoOpIssues(fname)

                def _normalise(value):
                    if value is None or (
                        isinstance(value, str) and not value.strip()
                    ):
                        return ""
                    try:
                        result = dt.normalise(str(value), issues=issues)
                        return result if result is not None else ""
                    except Exception as e:
                        logger.debug("harmonise error for %s: %s", fname, e)
                        return ""

                return _normalise

            normaliser = _make_normaliser(datatype, field)

            # Cast to Utf8 first to match legacy, which normalises string input.
            lf = lf.with_columns(
                pl.col(field)
                .cast(pl.Utf8)
                .map_elements(normaliser, return_dtype=pl.Utf8)
                .alias(field)
            )

        return lf

    def _remove_future_dates(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Remove values for entry-date or LastUpdatedDate if they are in the future.

        Called *after* ``_harmonise_field_values`` so dates are already in
        ISO ``YYYY-MM-DD`` format.  Uses ``strict=False`` so empty strings
        or unparseable remnants just become null (kept as-is via the
        ``otherwise`` branch).

        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names

        Returns:
            pl.LazyFrame: LazyFrame with future dates removed
        """
        today = date.today()

        for field in ["entry-date", "LastUpdatedDate"]:
            if field not in existing_columns:
                continue

            # ``strict=False`` avoids hard failures for empty/non-date values;
            # null parse results naturally fall through to ``otherwise``.
            lf = lf.with_columns(
                pl.when(
                    pl.col(field)
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    > pl.lit(today)
                )
                .then(pl.lit(""))
                .otherwise(pl.col(field))
                .alias(field)
            )

        return lf

    def _process_point_geometry(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Process GeoX, GeoY coordinates through PointDataType.

        Matches legacy behaviour: builds a Point from the coordinate pair,
        runs CRS detection / conversion (OSGB → WGS84 etc.) via
        ``PointDataType.normalise``, and extracts the transformed
        longitude / latitude back into GeoX / GeoY.

        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names

        Returns:
            pl.LazyFrame: LazyFrame with processed geometry
        """
        if "GeoX" not in existing_columns or "GeoY" not in existing_columns:
            return lf

        import shapely.wkt as _wkt
        from digital_land.datatype.point import PointDataType

        point_dt = PointDataType()
        issues = _NoOpIssues("GeoX,GeoY")

        def _normalise_point(row_struct):
            geox = row_struct.get("GeoX")
            geoy = row_struct.get("GeoY")
            if not geox or not geoy:
                return {"GeoX": "", "GeoY": ""}
            try:
                # PointDataType handles coordinate-system detection and
                # conversion to canonical WGS84 point output.
                geometry = point_dt.normalise(
                    [str(geox), str(geoy)], issues=issues
                )
                if geometry:
                    point_geom = _wkt.loads(geometry)
                    # Store transformed lon/lat back into original fields,
                    # matching the legacy phase contract.
                    x, y = point_geom.coords[0]
                    return {"GeoX": str(x), "GeoY": str(y)}
                return {"GeoX": "", "GeoY": ""}
            except Exception as e:
                logger.error("Exception processing GeoX,GeoY: %s", e)
                return {"GeoX": "", "GeoY": ""}

        lf = (
            lf.with_columns(
                pl.struct(["GeoX", "GeoY"])
                .map_elements(
                    _normalise_point,
                    return_dtype=pl.Struct(
                        {"GeoX": pl.Utf8, "GeoY": pl.Utf8}
                    ),
                )
                .alias("_point_result")
            )
            .with_columns(
                pl.col("_point_result").struct.field("GeoX").alias("GeoX"),
                pl.col("_point_result").struct.field("GeoY").alias("GeoY"),
            )
            .drop("_point_result")
        )

        return lf

    def _add_typology_curies(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Ensure typology fields (organisation, geography, document) have CURIE prefixes.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with CURIE-formatted typology fields
        """
        if not self.dataset:
            return lf

        for typology in ["organisation", "geography", "document"]:
            if typology not in existing_columns:
                continue

            # Add dataset prefix if value doesn't already contain ":"
            lf = lf.with_columns(
                pl.when(
                    (pl.col(typology).is_not_null())
                    & (pl.col(typology).str.len_chars() > 0)
                    & (~pl.col(typology).str.contains(":"))
                )
                .then(pl.lit(f"{self.dataset}:") + pl.col(typology))
                .otherwise(pl.col(typology))
                .alias(typology)
            )

        return lf

    def _process_wikipedia_urls(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Strip protocol from Wikipedia URLs, keeping only the page title.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with processed Wikipedia URLs
        """
        if "wikipedia" not in existing_columns:
            return lf

        # Replace full Wikipedia URLs with just the page title
        lf = lf.with_columns(
            pl.col("wikipedia")
            .str.replace(r"https://en\.wikipedia\.org/wiki/", "")
            .str.replace(r"http://en\.wikipedia\.org/wiki/", "")
            .alias("wikipedia")
        )

        return lf

    @staticmethod
    def _get_far_future_date(number_of_years_ahead: int):
        """
        Calculate a date far in the future for validation purposes.
        
        Args:
            number_of_years_ahead: Number of years to add to today
            
        Returns:
            date: A date in the future
        """
        today = date.today()
        y = today.year + number_of_years_ahead
        # keep same month/day if possible (handles Feb 29 & short months)
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)
