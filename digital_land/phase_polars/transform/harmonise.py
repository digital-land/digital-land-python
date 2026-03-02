import polars as pl
from datetime import date
from calendar import monthrange
import logging
import re
import duckdb

logger = logging.getLogger(__name__)

# NOTE: This module intentionally mirrors legacy stream harmonisation behaviour.
# Acceptance tests compare legacy and polars outputs field-by-field; comments
# below call out parity-sensitive decisions.
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
    """Stand-in for IssueLog that silently discards all messages.

    ``digital_land.datatype`` normalisers expect an ``issues`` object with
    ``log`` / ``log_issue``.  The polars path does not collect per-row
    telemetry yet, so this adapter preserves API compatibility.
    """

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
    """Apply harmonisation transformations to a Polars LazyFrame.

    Covers categorical normalisation, datatype conversion, future-date
    removal, GeoX/GeoY CRS conversion, typology CURIE prefixing, mandatory
    field checks, and Wikipedia URL stripping.  Mirrors the behaviour of the
    legacy stream-based ``HarmonisePhase`` in ``digital_land.phase.harmonise``.
    """

    def __init__(
        self,
        field_datatype_map=None,
        dataset=None,
        valid_category_values=None,
    ):
        self.field_datatype_map = field_datatype_map or {}
        self.dataset = dataset
        self.valid_category_values = valid_category_values or {}

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Apply all harmonisation transformations and return the result.

        Steps run in the same order as the legacy stream-based phase; some
        steps rely on earlier ones (e.g. future-date removal assumes dates are
        already in ISO ``YYYY-MM-DD`` form after datatype harmonisation).
        """
        if lf.collect_schema().len() == 0:
            return lf

        existing_columns = lf.collect_schema().names()

        lf = self._harmonise_categorical_fields(lf, existing_columns)
        lf = self._harmonise_field_values(lf, existing_columns)
        lf = self._remove_future_dates(lf, existing_columns)
        lf = self._process_point_geometry(lf, existing_columns)
        lf = self._add_typology_curies(lf, existing_columns)
        lf = self._check_mandatory_fields(lf, existing_columns)
        lf = self._process_wikipedia_urls(lf, existing_columns)

        # entry-number is an internal processing column and must not propagate.
        if "entry-number" in lf.collect_schema().names():
            lf = lf.drop("entry-number")

        return lf

    def _harmonise_categorical_fields(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """Normalise categorical fields against their allowed values.

        Matching is case-insensitive and treats spaces as interchangeable with
        hyphens (legacy parity).  Values not found in the allowed list are left
        unchanged.
        """
        for field, valid_values in self.valid_category_values.items():
            if field not in existing_columns:
                continue

            value_map = {v.lower().replace(" ", "-"): v for v in valid_values}

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
        """Return the canonical form of *value* from *value_map*, or *value* unchanged."""
        if not value or (isinstance(value, str) and not value.strip()):
            return value

        normalized = value.replace(" ", "-").lower()
        return value_map.get(normalized, value)

    def _harmonise_field_values(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """Apply datatype-based normalisation to every mapped field.

        Uses the same ``datatype.normalise()`` functions as the legacy phase
        to ensure identical output (datetime → ISO date, multipolygon → WGS84
        WKT, decimal → normalised string, etc.).  Spatial fields (multipolygon,
        point) are batched through DuckDB Spatial for performance.
        """
        from digital_land.datatype.factory import datatype_factory

        spatial_geometry_fields = []
        spatial_point_fields = []
        spatial_normalisers = {}

        for field in existing_columns:
            if field not in self.field_datatype_map:
                continue

            datatype_name = self.field_datatype_map[field]

            # Match legacy datetime bounds exactly.
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

            # Closure factory: each column gets its own datatype instance and
            # _NoOpIssues so lambda capture is stable across loop iterations.
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

            # Spatial fields cannot be normalised row-by-row via map_elements
            # because CRS detection needs the raw WKT string before any
            # conversion.  Collect them here and process in bulk via DuckDB.
            if datatype_name == "multipolygon":
                spatial_geometry_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue
            if datatype_name == "point":
                spatial_point_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue

            # Cast to Utf8 first — legacy always normalises from a string.
            lf = lf.with_columns(
                pl.col(field)
                .cast(pl.Utf8)
                .map_elements(normaliser, return_dtype=pl.Utf8)
                .alias(field)
            )

        if spatial_geometry_fields or spatial_point_fields:
            # DuckDB Spatial reprojects / validates geometry in bulk, then the
            # legacy datatype normaliser runs a final canonicalisation pass
            # (e.g. WKT whitespace normalisation) on the DuckDB output.
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

        # For each spatial field, classify the CRS of every value upfront so
        # we can drive a CASE expression inside a single DuckDB query rather
        # than reprojecting row-by-row in Python.
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
            # Start with all non-helper columns quoted; replace spatial field
            # expressions in-place below to preserve column ordering.
            select_parts = [
                f'"{column}"'
                for column in df.columns
                if column not in helper_cols
            ]

            for field in geometry_fields:
                srid_col = f"__dl_srid_{field}"
                flip_col = f"__dl_flip_{field}"
                geom_case = self._duckdb_geom_case(field, srid_col, flip_col)
                # ST_Multi wraps any geometry in a MULTIPOLYGON to match the
                # canonical WKT form expected by downstream consumers.
                # The ', ' → ',' replacement matches legacy WKT formatting.
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
                # Point fields are emitted as POINT WKT without forcing MULTI.
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
        """Clear entry-date / LastUpdatedDate if the value is in the future.

        Called after ``_harmonise_field_values`` so dates are already in
        ISO ``YYYY-MM-DD`` form.  ``strict=False`` means empty strings and
        unparseable values become null and fall through to the ``otherwise``
        branch unchanged.
        """
        today = date.today()

        for field in ["entry-date", "LastUpdatedDate"]:
            if field not in existing_columns:
                continue

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
        """Convert GeoX / GeoY to WGS84 longitude / latitude.

        Detects the source CRS (OSGB 27700, Web Mercator 3857, or WGS84 4326),
        reprojects via DuckDB Spatial, and writes the result back into GeoX/GeoY.
        """
        if "GeoX" not in existing_columns or "GeoY" not in existing_columns:
            return lf

        return self._normalise_geoxy_with_duckdb(lf)

    def _normalise_geoxy_with_duckdb(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Normalise GeoX/GeoY via DuckDB Spatial as primary path."""
        df = lf.collect().with_row_index("__dl_idx")

        geox_values = df.get_column("GeoX").to_list()
        geoy_values = df.get_column("GeoY").to_list()

        # Classify every (GeoX, GeoY) pair in Python so the DuckDB query can
        # branch on pre-computed SRID / flip flags rather than re-detecting CRS
        # inside SQL.
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
            # point_case reprojects to WGS84 and returns geometry; ST_X / ST_Y
            # then extract the final longitude / latitude respectively.
            # Values with an unrecognised CRS (srid = '') are set to ''.
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
    def _degrees_like(x, y) -> bool:
        """Return True if (x, y) look like WGS84 decimal degrees (EPSG:4326)."""
        return -60.0 < x < 60.0 and -60.0 < y < 60.0

    @staticmethod
    def _easting_northing_like(x, y) -> bool:
        """Return True if (x, y) look like OSGB36 easting / northing (EPSG:27700)."""
        return 1000.0 < x < 1000000.0 and 1000.0 < y < 1000000.0

    @staticmethod
    def _metres_like(x, y) -> bool:
        """Return True if (x, y) look like Web Mercator metres (EPSG:3857)."""
        return 6000000.0 < y < 10000000.0

    def _classify_xy_crs(self, x, y):
        """Return ``(srid, flip)`` for a raw (x, y) coordinate pair.

        *srid* is one of ``'4326'``, ``'27700'``, ``'3857'``, or ``''`` when
        the CRS cannot be determined.  *flip* is ``True`` when the coordinates
        appear to be supplied in (latitude, longitude) order rather than the
        conventional (longitude, latitude) / (easting, northing) order.
        """
        try:
            x = float(str(x).strip())
            y = float(str(y).strip())
        except Exception:
            return "", False

        # Check the most common CRS first; fall back through less common ones.
        if self._degrees_like(x, y):
            return "4326", False
        if self._degrees_like(y, x):  # lat/lon supplied as lon/lat
            return "4326", True
        if self._easting_northing_like(x, y):
            return "27700", False
        if self._easting_northing_like(y, x):  # northing/easting order
            return "27700", True
        if self._metres_like(x, y):
            return "3857", False
        if self._metres_like(y, x):
            return "3857", True
        return "", False

    def _classify_wkt_crs_with_flip(self, wkt_value):
        """Return ``(srid, flip)`` by extracting the first coordinate pair from *wkt_value*.

        Delegates to ``_classify_xy_crs`` after parsing the first two numeric
        tokens from the WKT string.  Returns ``('', False)`` for null / empty
        input or strings with fewer than two numbers.
        """
        if wkt_value is None:
            return "", False
        text = str(wkt_value).strip()
        if not text:
            return "", False

        # Extract numeric tokens from the WKT (handles POINT, POLYGON, etc.).
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
        """Build a DuckDB CASE expression that reprojects *field* to EPSG:4326.

        The expression reads the pre-computed *srid_col* / *flip_col* helper
        columns to decide how to parse and transform the WKT geometry:
        - EPSG:4326: already in WGS84; flip coordinates if supplied lat/lon.
        - EPSG:27700: transform from OSGB36 to WGS84 then flip to lon/lat.
        - EPSG:3857: transform from Web Mercator to WGS84 then flip to lon/lat.
        Returns NULL for unrecognised CRS or unparseable geometry.
        """
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
        """Prefix bare typology values with ``<dataset>:`` to form CURIEs.

        Applies to ``organisation``, ``geography``, and ``document`` columns.
        Values that already contain ":" are left unchanged.
        """
        if not self.dataset:
            return lf

        for typology in ["organisation", "geography", "document"]:
            if typology not in existing_columns:
                continue

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

    def _check_mandatory_fields(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """Log ``missing value`` issues for empty mandatory fields.

        Mirrors legacy behaviour, including the geometry/point co-constraint:
        if either column exists in the data, at least one must be non-empty.

        Issue logging is currently a no-op (``_NoOpIssues``); this method
        provides structural parity and is ready for a real issue-log once one
        is wired into the polars pipeline.
        """
        mandatory_fields = MANDATORY_FIELDS_DICT.get(self.dataset)

        # geometry and point are checked as a co-constraint regardless of
        # whether the dataset has other mandatory fields.
        has_geometry_or_point = any(
            f in existing_columns for f in ["geometry", "point"]
        )

        if not has_geometry_or_point and not mandatory_fields:
            return lf

        issues = _NoOpIssues()
        df = lf.collect()

        for row in df.iter_rows(named=True):
            for field in existing_columns:
                if field in ["geometry", "point"]:
                    # Co-constraint: at least one of geometry / point must be
                    # present.  Log the issue on whichever column is being
                    # iterated to mirror legacy per-field issue reporting.
                    geom_empty = not row.get("geometry")
                    point_empty = not row.get("point")
                    if geom_empty and point_empty:
                        issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )
                elif mandatory_fields and field in mandatory_fields:
                    if not row.get(field):
                        issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )

        return df.lazy()

    def _process_wikipedia_urls(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """Strip the ``https://en.wikipedia.org/wiki/`` prefix, keeping only the page title."""
        if "wikipedia" not in existing_columns:
            return lf

        lf = lf.with_columns(
            pl.col("wikipedia")
            .str.replace(r"https://en\.wikipedia\.org/wiki/", "")
            .str.replace(r"http://en\.wikipedia\.org/wiki/", "")
            .alias("wikipedia")
        )

        return lf

    @staticmethod
    def _get_far_future_date(number_of_years_ahead: int) -> date:
        """Return today's date shifted forward by *number_of_years_ahead* years.

        Handles Feb 29 and short months by clamping the day to the last valid
        day of the target month.
        """
        today = date.today()
        y = today.year + number_of_years_ahead
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)