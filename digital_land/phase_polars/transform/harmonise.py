import polars as pl
from datetime import date
from calendar import monthrange
import logging
import re
import duckdb

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
FIRST_COORD_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


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

        spatial_geometry_fields = []
        spatial_point_fields = []
        spatial_normalisers = {}

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

            if datatype_name == "multipolygon":
                spatial_geometry_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue
            if datatype_name == "point":
                spatial_point_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue

            # Cast to Utf8 first to match legacy, which normalises string input.
            lf = lf.with_columns(
                pl.col(field)
                .cast(pl.Utf8)
                .map_elements(normaliser, return_dtype=pl.Utf8)
                .alias(field)
            )

        if spatial_geometry_fields or spatial_point_fields:
            lf = self._normalise_spatial_fields_with_duckdb(
                lf,
                geometry_fields=spatial_geometry_fields,
                point_fields=spatial_point_fields,
            )
            lf = self._canonicalise_spatial_fields(lf, spatial_normalisers)

        return lf

    def _canonicalise_spatial_fields(
        self, lf: pl.LazyFrame, normalisers: dict
    ) -> pl.LazyFrame:
        """Apply legacy datatype canonicalisation to DuckDB spatial output."""
        if not normalisers:
            return lf

        df = lf.collect()
        updates = []

        for field, normaliser in normalisers.items():
            values = df.get_column(field).to_list()
            updates.append(
                pl.Series(field, [normaliser(value) for value in values], dtype=pl.Utf8)
            )

        return df.with_columns(updates).lazy()

    def _normalise_spatial_fields_with_duckdb(
        self,
        lf: pl.LazyFrame,
        geometry_fields: list,
        point_fields: list,
    ) -> pl.LazyFrame:
        """Normalise multipolygon/point fields via DuckDB Spatial as primary path."""
        if not geometry_fields and not point_fields:
            return lf

        df = lf.collect().with_row_index("__dl_idx")

        helper_cols = ["__dl_idx"]

        for field in geometry_fields + point_fields:
            values = df.get_column(field).to_list()
            srids: list[str] = []
            flips: list[bool] = []
            for value in values:
                srid, flip = self._classify_wkt_crs_with_flip(value)
                srids.append(srid)
                flips.append(flip)

            srid_col = f"__dl_srid_{field}"
            flip_col = f"__dl_flip_{field}"
            helper_cols.extend([srid_col, flip_col])
            df = df.with_columns(
                pl.Series(srid_col, srids, dtype=pl.Utf8),
                pl.Series(flip_col, flips, dtype=pl.Boolean),
            )

        con = self._duckdb_spatial_connection()
        con.register("dl_spatial", df.to_arrow())

        try:
            select_parts = [
                f'"{column}"'
                for column in df.columns
                if column not in helper_cols
            ]

            for field in geometry_fields:
                srid_col = f"__dl_srid_{field}"
                flip_col = f"__dl_flip_{field}"
                geom_case = self._duckdb_geom_case(field, srid_col, flip_col)
                expr = (
                    f"CASE "
                    f"WHEN \"{field}\" IS NULL OR trim(\"{field}\") = '' THEN '' "
                    f"ELSE coalesce(replace(ST_AsText(ST_Multi({geom_case})), ', ', ','), '') "
                    f"END AS \"{field}\""
                )
                select_parts[select_parts.index(f'"{field}"')] = expr

            for field in point_fields:
                srid_col = f"__dl_srid_{field}"
                flip_col = f"__dl_flip_{field}"
                geom_case = self._duckdb_geom_case(field, srid_col, flip_col)
                expr = (
                    f"CASE "
                    f"WHEN \"{field}\" IS NULL OR trim(\"{field}\") = '' THEN '' "
                    f"ELSE coalesce(ST_AsText({geom_case}), '') "
                    f"END AS \"{field}\""
                )
                select_parts[select_parts.index(f'"{field}"')] = expr

            query = (
                "SELECT "
                + ", ".join(select_parts)
                + " FROM dl_spatial ORDER BY __dl_idx"
            )

            return pl.from_arrow(con.execute(query).arrow()).lazy()
        finally:
            con.close()

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

        return self._normalise_geoxy_with_duckdb(lf)

    def _normalise_geoxy_with_duckdb(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Normalise GeoX/GeoY via DuckDB Spatial as primary path."""
        df = lf.collect().with_row_index("__dl_idx")

        geox_values = df.get_column("GeoX").to_list()
        geoy_values = df.get_column("GeoY").to_list()

        srids: list[str] = []
        flips: list[bool] = []
        for geox, geoy in zip(geox_values, geoy_values):
            srid, flip = self._classify_xy_crs(geox, geoy)
            srids.append(srid)
            flips.append(flip)

        df = df.with_columns(
            pl.Series("__dl_point_srid", srids, dtype=pl.Utf8),
            pl.Series("__dl_point_flip", flips, dtype=pl.Boolean),
        )

        con = self._duckdb_spatial_connection()
        con.register("dl_points", df.to_arrow())

        try:
            point_case = (
                "CASE "
                "WHEN __dl_point_srid = '4326' AND __dl_point_flip = FALSE "
                "THEN ST_Point(TRY_CAST(\"GeoX\" AS DOUBLE), TRY_CAST(\"GeoY\" AS DOUBLE)) "
                "WHEN __dl_point_srid = '4326' AND __dl_point_flip = TRUE "
                "THEN ST_Point(TRY_CAST(\"GeoY\" AS DOUBLE), TRY_CAST(\"GeoX\" AS DOUBLE)) "
                "WHEN __dl_point_srid = '27700' AND __dl_point_flip = FALSE "
                "THEN ST_FlipCoordinates(ST_Transform(ST_Point(TRY_CAST(\"GeoX\" AS DOUBLE), TRY_CAST(\"GeoY\" AS DOUBLE)), 'EPSG:27700', 'EPSG:4326')) "
                "WHEN __dl_point_srid = '27700' AND __dl_point_flip = TRUE "
                "THEN ST_FlipCoordinates(ST_Transform(ST_Point(TRY_CAST(\"GeoY\" AS DOUBLE), TRY_CAST(\"GeoX\" AS DOUBLE)), 'EPSG:27700', 'EPSG:4326')) "
                "WHEN __dl_point_srid = '3857' AND __dl_point_flip = FALSE "
                "THEN ST_FlipCoordinates(ST_Transform(ST_Point(TRY_CAST(\"GeoX\" AS DOUBLE), TRY_CAST(\"GeoY\" AS DOUBLE)), 'EPSG:3857', 'EPSG:4326')) "
                "WHEN __dl_point_srid = '3857' AND __dl_point_flip = TRUE "
                "THEN ST_FlipCoordinates(ST_Transform(ST_Point(TRY_CAST(\"GeoY\" AS DOUBLE), TRY_CAST(\"GeoX\" AS DOUBLE)), 'EPSG:3857', 'EPSG:4326')) "
                "ELSE NULL END"
            )

            query = (
                "SELECT * EXCLUDE (__dl_idx, __dl_point_srid, __dl_point_flip), "
                "CASE "
                "WHEN \"GeoX\" IS NULL OR \"GeoY\" IS NULL OR trim(CAST(\"GeoX\" AS VARCHAR)) = '' OR trim(CAST(\"GeoY\" AS VARCHAR)) = '' OR __dl_point_srid = '' "
                "THEN '' "
                f"ELSE coalesce(CAST(round(ST_X({point_case}), 6) AS VARCHAR), '') END AS \"GeoX\", "
                "CASE "
                "WHEN \"GeoX\" IS NULL OR \"GeoY\" IS NULL OR trim(CAST(\"GeoX\" AS VARCHAR)) = '' OR trim(CAST(\"GeoY\" AS VARCHAR)) = '' OR __dl_point_srid = '' "
                "THEN '' "
                f"ELSE coalesce(CAST(round(ST_Y({point_case}), 6) AS VARCHAR), '') END AS \"GeoY\" "
                "FROM dl_points ORDER BY __dl_idx"
            )

            return pl.from_arrow(con.execute(query).arrow()).lazy()
        finally:
            con.close()

    @staticmethod
    def _duckdb_spatial_connection():
        """Create a DuckDB connection with spatial extension loaded."""
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("LOAD spatial")
        except Exception:
            con.execute("INSTALL spatial")
            con.execute("LOAD spatial")
        return con

    @staticmethod
    def _degrees_like(x, y):
        return -60.0 < x < 60.0 and -60.0 < y < 60.0

    @staticmethod
    def _easting_northing_like(x, y):
        return 1000.0 < x < 1000000.0 and 1000.0 < y < 1000000.0

    @staticmethod
    def _metres_like(x, y):
        return 6000000.0 < y < 10000000.0

    def _classify_xy_crs(self, x, y):
        try:
            x = float(str(x).strip())
            y = float(str(y).strip())
        except Exception:
            return "", False

        if self._degrees_like(x, y):
            return "4326", False
        if self._degrees_like(y, x):
            return "4326", True
        if self._easting_northing_like(x, y):
            return "27700", False
        if self._easting_northing_like(y, x):
            return "27700", True
        if self._metres_like(x, y):
            return "3857", False
        if self._metres_like(y, x):
            return "3857", True
        return "", False

    def _classify_wkt_crs_with_flip(self, wkt_value):
        if wkt_value is None:
            return "", False
        text = str(wkt_value).strip()
        if not text:
            return "", False

        nums = FIRST_COORD_RE.findall(text)
        if len(nums) < 2:
            return "", False
        try:
            x = float(nums[0])
            y = float(nums[1])
        except Exception:
            return "", False

        return self._classify_xy_crs(x, y)

    @staticmethod
    def _duckdb_geom_case(field: str, srid_col: str, flip_col: str) -> str:
        geom = f'TRY(ST_GeomFromText("{field}"))'
        return (
            "CASE "
            f"WHEN \"{srid_col}\" = '4326' AND \"{flip_col}\" = FALSE THEN {geom} "
            f"WHEN \"{srid_col}\" = '4326' AND \"{flip_col}\" = TRUE THEN ST_FlipCoordinates({geom}) "
            f"WHEN \"{srid_col}\" = '27700' AND \"{flip_col}\" = FALSE THEN ST_FlipCoordinates(ST_Transform({geom}, 'EPSG:27700', 'EPSG:4326')) "
            f"WHEN \"{srid_col}\" = '27700' AND \"{flip_col}\" = TRUE THEN ST_FlipCoordinates(ST_Transform(ST_FlipCoordinates({geom}), 'EPSG:27700', 'EPSG:4326')) "
            f"WHEN \"{srid_col}\" = '3857' AND \"{flip_col}\" = FALSE THEN ST_FlipCoordinates(ST_Transform({geom}, 'EPSG:3857', 'EPSG:4326')) "
            f"WHEN \"{srid_col}\" = '3857' AND \"{flip_col}\" = TRUE THEN ST_FlipCoordinates(ST_Transform(ST_FlipCoordinates({geom}), 'EPSG:3857', 'EPSG:4326')) "
            "ELSE NULL END"
        )

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
