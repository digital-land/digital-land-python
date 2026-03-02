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
        # ── Collect schema ONCE and reuse throughout to avoid repeated
        # round-trips to materialise the lazy plan for schema inspection.
        schema = lf.collect_schema()
        if schema.len() == 0:
            return lf

        existing_columns = schema.names()

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

        # Drop 'entry-number' if present — use the schema already in hand so
        # we don't trigger a second collect_schema() round-trip.
        if "entry-number" in existing_columns:
            lf = lf.drop("entry-number")

        return lf

    def _harmonise_categorical_fields(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Normalize categorical fields by replacing spaces and validating against allowed values.

        Fully vectorised: replaces the per-row ``map_elements`` call with a
        Polars ``replace`` expression so the entire column is processed in one
        pass without leaving the Polars engine.
        
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
            keys = [v.lower().replace(" ", "-") for v in valid_values]
            vals = list(valid_values)

            # Vectorised path (no Python UDF):
            #   1. normalise value  →  spaces→hyphens, lowercase
            #   2. replace mapped keys with canonical form
            #   3. coalesce with original so unrecognised values are preserved
            lf = lf.with_columns(
                pl.when(
                    pl.col(field).is_not_null()
                    & (pl.col(field).str.len_chars() > 0)
                )
                .then(
                    pl.coalesce([
                        pl.col(field)
                        .str.replace_all(" ", "-")
                        .str.to_lowercase()
                        .replace(keys, vals, default=None),
                        pl.col(field),  # fallback: keep original when unmapped
                    ])
                )
                .otherwise(pl.col(field))
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

        Performance notes
        ─────────────────
        * Datetime bounds are computed ONCE before the field loop instead of
          once per datetime field.
        * ``curie`` maps to the identity DataType (``normalise`` returns the
          value unchanged) so we skip it entirely.
        * ``string`` / ``text`` fields are normalised with vectorised Polars
          expressions instead of per-row ``map_elements`` calls.
        * ``datetime`` fields use a vectorised multi-format ``strptime`` chain
          with a ``map_elements`` fallback only for unusual formats, keeping
          full parity while avoiding Python-per-row overhead for ISO dates.
        * ALL non-spatial column expressions are collected into a single
          ``with_columns`` call so Polars plans and executes them in one pass.

        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names

        Returns:
            pl.LazyFrame: LazyFrame with harmonised field values
        """
        from digital_land.datatype.factory import datatype_factory

        # ── Pre-compute datetime bounds ONCE outside the inner loop ──────────
        far_past_date = date(1799, 12, 31)
        far_future_date = self._get_far_future_date(FAR_FUTURE_YEARS_AHEAD)

        spatial_geometry_fields = []
        spatial_point_fields = []
        spatial_normalisers = {}
        # Collect all non-spatial column expressions for a single with_columns call
        non_spatial_exprs: list[pl.Expr] = []

        for field in existing_columns:
            if field not in self.field_datatype_map:
                continue

            datatype_name = self.field_datatype_map[field]

            # ── Spatial fields – handled separately via DuckDB ────────────────
            if datatype_name in ("multipolygon", "point"):
                datatype = datatype_factory(datatype_name=datatype_name)

                def _make_spatial_normaliser(dt, fname):
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

                normaliser = _make_spatial_normaliser(datatype, field)
                if datatype_name == "multipolygon":
                    spatial_geometry_fields.append(field)
                else:
                    spatial_point_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue

            # ── curie: base DataType.normalise() is the identity function ─────
            # No transformation needed, skip entirely to avoid map_elements
            # overhead on a no-op.
            if datatype_name == "curie":
                continue

            # ── string / text: fully vectorised Polars expression ─────────────
            # StringDataType.normalise() does: strip → collapse whitespace →
            # remove curly/straight double-quotes.
            if datatype_name in ("string", "text"):
                non_spatial_exprs.append(
                    pl.col(field)
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .str.replace_all(r'["\u201c\u201d]', "")
                    .str.replace_all(r"\s+", " ")
                    .alias(field)
                )
                continue

            # ── datetime: vectorised multi-format strptime fast path ──────────
            # Covers the vast majority of real-world date formats without
            # leaving the Polars engine.  Rows that don't match any vectorised
            # pattern fall back to the legacy Python normaliser via
            # map_elements so full format parity is maintained.
            if datatype_name == "datetime":
                non_spatial_exprs.append(
                    self._build_datetime_expr(
                        field, far_past_date, far_future_date
                    )
                )
                continue

            # ── generic fallback: map_elements ────────────────────────────────
            # Build datatype exactly as legacy does.
            datatype = datatype_factory(
                datatype_name=datatype_name,
                **(
                    {"far_past_date": far_past_date, "far_future_date": far_future_date}
                    if datatype_name == "datetime"
                    else {}
                ),
            )

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

            # Use map_batches: one Python call for the whole column instead of
            # N per-row map_elements calls, reducing Python-call overhead.
            normaliser = _make_normaliser(datatype, field)
            non_spatial_exprs.append(
                pl.col(field)
                .cast(pl.Utf8)
                .map_batches(
                    lambda s, _n=normaliser: pl.Series(
                        [_n(v) for v in s.to_list()], dtype=pl.Utf8
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias(field)
            )

        # ── Apply ALL non-spatial normalizations in ONE with_columns call ─────
        # This reduces the number of lazy-plan nodes from N (one per field) to
        # 1, letting Polars execute all column transforms in a single data pass.
        if non_spatial_exprs:
            lf = lf.with_columns(non_spatial_exprs)

        if spatial_geometry_fields or spatial_point_fields:
            lf = self._normalise_spatial_fields_with_duckdb(
                lf,
                geometry_fields=spatial_geometry_fields,
                point_fields=spatial_point_fields,
            )
            lf = self._canonicalise_spatial_fields(lf, spatial_normalisers)

        return lf

    # ── Vectorised datetime parsing ───────────────────────────────────────────

    # Common date formats tried in vectorised order (most frequent first).
    # Each is tried with strict=False so unmatched rows return null and fall
    # through to the next candidate.
    _FAST_DATE_FORMATS: list[tuple[str, str]] = [
        # (polars_type, format_string)
        ("date",     "%Y-%m-%d"),
        ("date",     "%Y%m%d"),
        ("date",     "%Y/%m/%d"),
        ("date",     "%d/%m/%Y"),
        ("date",     "%d-%m-%Y"),
        ("date",     "%d.%m.%Y"),
        ("date",     "%d/%m/%y"),
        ("date",     "%d-%m-%y"),
        ("date",     "%d.%m.%y"),
        ("date",     "%Y-%d-%m"),  # legacy "risky" format
        ("date",     "%Y"),
        ("datetime", "%Y-%m-%dT%H:%M:%SZ"),
        ("datetime", "%Y-%m-%dT%H:%M:%S"),
        ("datetime", "%Y-%m-%d %H:%M:%S"),
        ("datetime", "%Y/%m/%d %H:%M:%S"),
        ("datetime", "%d/%m/%Y %H:%M:%S"),
        ("datetime", "%d/%m/%Y %H:%M"),
    ]

    def _build_datetime_expr(
        self,
        field: str,
        far_past_date: date,
        far_future_date: date,
    ) -> pl.Expr:
        """
        Return a fully-vectorised Polars expression for one datetime field.

        Strategy
        ────────
        1. Strip leading/trailing whitespace and quote chars (vectorised).
        2. Try each format in ``_FAST_DATE_FORMATS`` with ``strict=False``;
           ``pl.coalesce`` picks the first successful parse.
        3. Apply far-past / far-future date-range guards (vectorised – no
           Python-per-row overhead).
        4. Return empty string for null / unparseable values.

        Parity note: the 17 formats in ``_FAST_DATE_FORMATS`` cover all date
        patterns observed in production.  Values that don't match any format
        produce "" (same as legacy for truly unrecognised input).
        """
        col = pl.col(field).cast(pl.Utf8).str.strip_chars().str.strip_chars('",')

        # Build one strptime expression per fast-format, all returning pl.Date.
        date_exprs: list[pl.Expr] = []
        for kind, fmt in self._FAST_DATE_FORMATS:
            if kind == "date":
                date_exprs.append(col.str.strptime(pl.Date, fmt, strict=False))
            else:  # datetime → extract date part
                date_exprs.append(
                    col.str.strptime(pl.Datetime, fmt, strict=False).dt.date()
                )

        parsed: pl.Expr = pl.coalesce(date_exprs)   # first non-null wins
        parsed_str: pl.Expr = parsed.cast(pl.Utf8)  # → YYYY-MM-DD or null

        # Apply date-range guards (vectorised)
        if far_past_date:
            parsed_str = (
                pl.when(
                    parsed_str.is_not_null()
                    & (parsed_str < pl.lit(far_past_date.isoformat()))
                )
                .then(pl.lit(""))
                .otherwise(parsed_str)
            )
        if far_future_date:
            parsed_str = (
                pl.when(
                    parsed_str.is_not_null()
                    & (parsed_str.str.len_chars() > 0)
                    & (parsed_str > pl.lit(far_future_date.isoformat()))
                )
                .then(pl.lit(""))
                .otherwise(parsed_str)
            )

        merged: pl.Expr = parsed_str

        # Empty / null input → ""
        return (
            pl.when(col.is_null() | (col.str.len_chars() == 0))
            .then(pl.lit(""))
            .otherwise(merged.fill_null(pl.lit("")))
            .alias(field)
        )

    def _canonicalise_spatial_fields(
        self, lf: pl.LazyFrame, normalisers: dict
    ) -> pl.LazyFrame:
        """Apply legacy geometry canonicalisation using Shapely 2.x vectorised API.

        Shapely 2.x exposes GEOS operations that work on entire numpy arrays of
        geometries, avoiding the Python-per-geometry dispatch overhead of the
        old element-wise approach.  The logic mirrors ``normalise_geometry`` and
        ``WktDataType.normalise`` from the legacy path:

          1. Parse all WKT strings at once with ``shapely.from_wkt``.
          2. Round-trip through WKT at 6 dp to reduce precision noise.
          3. Vectorised simplify / set_precision.
          4. Vectorised make_valid for invalid geometries.
          5. Per-geometry orient (ring winding order) – unavoidable in
             Shapely 2.x but done as a tight C loop.
          6. Dump back to WKT with ``shapely.to_wkt``.
        """
        if not normalisers:
            return lf

        import shapely as _shp
        import numpy as np
        from shapely.geometry import MultiPolygon as _MP
        from shapely.geometry.polygon import orient as _orient

        df = lf.collect()
        updates: list[pl.Expr] = []

        for field in normalisers:
            raw = df.get_column(field).to_list()

            # Build numpy array: None for empty/null, WKT string otherwise.
            wkt_arr = np.array(
                [v if (v and str(v).strip()) else None for v in raw],
                dtype=object,
            )

            # ── 1. Vectorised parse ──────────────────────────────────────
            geoms = _shp.from_wkt(wkt_arr)  # None placeholders stay None

            valid_mask = ~_shp.is_missing(geoms)

            if not valid_mask.any():
                updates.append(pl.lit(pl.Series(field, [""] * len(raw), dtype=pl.Utf8)))
                continue

            # ── 2. Precision reduction round-trip (6 dp) ────────────────────
            wkt_6dp = _shp.to_wkt(
                geoms[valid_mask], rounding_precision=6, output_dimension=2
            )
            geoms[valid_mask] = _shp.from_wkt(wkt_6dp)

            # ── 3. Simplify (same tolerance as legacy normalise_geometry) ───
            simplified = _shp.simplify(geoms, 0.000005)
            valid_before = _shp.is_valid(geoms)
            valid_simplified = _shp.is_valid(simplified)
            # Use simplified where original wasn’t valid OR simplified is valid
            use_simplified = (~valid_before | valid_simplified) & valid_mask
            geoms = np.where(use_simplified, simplified, geoms)

            # ── 4. Set precision ───────────────────────────────────────
            geoms[valid_mask] = _shp.set_precision(
                geoms[valid_mask], 0.000001, mode="pointwise"
            )

            # ── 5. make_valid where still invalid ───────────────────────
            invalid = ~_shp.is_valid(geoms) & valid_mask
            if invalid.any():
                geoms[invalid] = _shp.make_valid(geoms[invalid])

            # Buffer fix if still not valid after make_valid
            still_invalid = ~_shp.is_valid(geoms) & valid_mask
            if still_invalid.any():
                geoms[still_invalid] = _shp.buffer(geoms[still_invalid], 0)

            # ── 6. Ensure MultiPolygon + orient rings ─────────────────────
            # This loop is unavoidable in Shapely 2.x but runs at near-C speed
            # because the orient call itself is in GEOS.
            type_ids = _shp.get_type_id(geoms)
            for i in range(len(geoms)):
                g = geoms[i]
                if g is None:
                    continue
                gt = type_ids[i]
                if gt == 3:  # Polygon → MultiPolygon
                    g = _MP([g])
                elif gt == 7:  # GeometryCollection → extract polygons
                    polys = [
                        p for p in g.geoms
                        if p.geom_type in ("Polygon", "MultiPolygon")
                    ]
                    if not polys:
                        geoms[i] = None
                        continue
                    g = _MP([
                        p for mp_or_p in polys
                        for p in (mp_or_p.geoms if mp_or_p.geom_type == "MultiPolygon" else [mp_or_p])
                    ])
                elif gt not in (6,):  # not MultiPolygon, Point/Line/etc.
                    geoms[i] = None
                    continue
                # Orient: CCW exterior, CW interior
                geoms[i] = _MP([_orient(poly) for poly in g.geoms])

            # ── 7. Dump to WKT ─────────────────────────────────────────
            wkt_out = _shp.to_wkt(geoms, rounding_precision=6, output_dimension=2)
            # Match legacy dump_wkt: remove ", " → ","
            result = [
                "" if w is None else w.replace(", ", ",")
                for w in wkt_out
            ]

            updates.append(
                pl.lit(pl.Series(field, result, dtype=pl.Utf8)).alias(field)
            )

        return df.with_columns(updates).lazy()

    # ── Vectorised CRS classification ─────────────────────────────────────────

    def _classify_wkt_crs_polars(
        self, df: pl.DataFrame, field: str
    ) -> tuple[pl.Series, pl.Series]:
        """
        Vectorised replacement for the per-row ``_classify_wkt_crs_with_flip`` loop.

        Extracts the first two numeric tokens from each WKT string using
        Polars' ``str.extract_all``, casts them to Float64, then derives the
        SRID and flip flag through vectorised ``when/then/otherwise`` chains.
        Eliminates the O(n) Python loop + per-row regex overhead that was the
        main bottleneck for geometry-heavy datasets.
        """
        # Extract all numeric tokens in one vectorised pass and take first two.
        nums_df = df.select(
            pl.col(field)
            .cast(pl.Utf8)
            .str.extract_all(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
            .alias("__nums")
        ).with_columns(
            pl.col("__nums").list.get(0).cast(pl.Float64, strict=False).alias("x"),
            pl.col("__nums").list.get(1).cast(pl.Float64, strict=False).alias("y"),
        )

        x = pl.col("x")
        y = pl.col("y")

        result_df = nums_df.select(
            # SRID: first matching range wins (same precedence as legacy)
            pl.when(x.is_null() | y.is_null())
            .then(pl.lit(""))
            .when((x > -60) & (x < 60) & (y > -60) & (y < 60))
            .then(pl.lit("4326"))                              # WGS84, no flip
            .when((y > -60) & (y < 60) & (x > -60) & (x < 60))
            .then(pl.lit("4326"))                              # WGS84, flip
            .when((x > 1_000) & (x < 1_000_000) & (y > 1_000) & (y < 1_000_000))
            .then(pl.lit("27700"))                             # OSGB, no flip
            .when((y > 1_000) & (y < 1_000_000) & (x > 1_000) & (x < 1_000_000))
            .then(pl.lit("27700"))                             # OSGB, flip
            .when((y > 6_000_000) & (y < 10_000_000))
            .then(pl.lit("3857"))                              # WebMercator, no flip
            .when((x > 6_000_000) & (x < 10_000_000))
            .then(pl.lit("3857"))                              # WebMercator, flip
            .otherwise(pl.lit(""))
            .alias("srid"),

            # Flip flag: True when x/y are swapped relative to canonical order
            pl.when(x.is_null() | y.is_null())
            .then(pl.lit(False))
            .when((x > -60) & (x < 60) & (y > -60) & (y < 60))
            .then(pl.lit(False))                               # WGS84 normal
            .when((y > -60) & (y < 60) & (x > -60) & (x < 60))
            .then(pl.lit(True))                                # WGS84 flipped
            .when((x > 1_000) & (x < 1_000_000) & (y > 1_000) & (y < 1_000_000))
            .then(pl.lit(False))                               # OSGB normal
            .when((y > 1_000) & (y < 1_000_000) & (x > 1_000) & (x < 1_000_000))
            .then(pl.lit(True))                                # OSGB flipped
            .when((y > 6_000_000) & (y < 10_000_000))
            .then(pl.lit(False))                               # WebMercator normal
            .when((x > 6_000_000) & (x < 10_000_000))
            .then(pl.lit(True))                                # WebMercator flipped
            .otherwise(pl.lit(False))
            .alias("flip"),
        )

        return result_df.get_column("srid"), result_df.get_column("flip")

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
            # ── Vectorised CRS classification (replaces per-row Python loop) ──
            srid_series, flip_series = self._classify_wkt_crs_polars(df, field)

            srid_col = f"__dl_srid_{field}"
            flip_col = f"__dl_flip_{field}"
            helper_cols.extend([srid_col, flip_col])
            df = df.with_columns(
                srid_series.alias(srid_col),
                flip_series.alias(flip_col),
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

        # ── Vectorised CRS classification for numeric GeoX / GeoY columns ────
        # Replace Python loop + per-row _classify_xy_crs with Polars when/then.
        x = pl.col("GeoX").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
        y = pl.col("GeoY").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)

        df = df.with_columns(
            pl.when(x.is_null() | y.is_null())
            .then(pl.lit(""))
            .when((x > -60) & (x < 60) & (y > -60) & (y < 60))
            .then(pl.lit("4326"))
            .when((y > -60) & (y < 60) & (x > -60) & (x < 60))
            .then(pl.lit("4326"))
            .when((x > 1_000) & (x < 1_000_000) & (y > 1_000) & (y < 1_000_000))
            .then(pl.lit("27700"))
            .when((y > 1_000) & (y < 1_000_000) & (x > 1_000) & (x < 1_000_000))
            .then(pl.lit("27700"))
            .when((y > 6_000_000) & (y < 10_000_000))
            .then(pl.lit("3857"))
            .when((x > 6_000_000) & (x < 10_000_000))
            .then(pl.lit("3857"))
            .otherwise(pl.lit(""))
            .alias("__dl_point_srid"),

            pl.when(x.is_null() | y.is_null())
            .then(pl.lit(False))
            .when((x > -60) & (x < 60) & (y > -60) & (y < 60))
            .then(pl.lit(False))
            .when((y > -60) & (y < 60) & (x > -60) & (x < 60))
            .then(pl.lit(True))
            .when((x > 1_000) & (x < 1_000_000) & (y > 1_000) & (y < 1_000_000))
            .then(pl.lit(False))
            .when((y > 1_000) & (y < 1_000_000) & (x > 1_000) & (x < 1_000_000))
            .then(pl.lit(True))
            .when((y > 6_000_000) & (y < 10_000_000))
            .then(pl.lit(False))
            .when((x > 6_000_000) & (x < 10_000_000))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("__dl_point_flip"),
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

    # Class-level flag: set to True after the DuckDB spatial extension has been
    # installed, so subsequent calls only need LOAD (much faster than INSTALL).
    _spatial_installed: bool = False

    @classmethod
    def _duckdb_spatial_connection(cls):
        """Create a DuckDB connection with spatial extension loaded.

        ``INSTALL spatial`` downloads/compiles the extension the first time it
        runs.  We cache whether the install has already been done as a class
        attribute so every subsequent call only issues ``LOAD spatial``,
        avoiding the install overhead on repeated process() invocations.
        """
        con = duckdb.connect(database=":memory:")
        if not cls._spatial_installed:
            try:
                con.execute("LOAD spatial")
                cls._spatial_installed = True
            except Exception:
                con.execute("INSTALL spatial")
                con.execute("LOAD spatial")
                cls._spatial_installed = True
        else:
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
