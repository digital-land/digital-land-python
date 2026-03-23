from __future__ import annotations

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

        # Polars datetime formats mirror legacy datetime parsing order from
        # digital_land.datatype.date.DateDataType.normalise.
        self._DATETIME_FORMATS = [
            ("date", "%Y-%m-%d"),
            ("date", "%Y%m%d"),
            ("datetime", "%Y/%m/%d %H:%M:%S%z"),
            ("datetime", "%Y/%m/%d %H:%M:%S+00"),
            ("datetime", "%Y/%m/%d %H:%M:%S"),
            ("datetime", "%Y/%m/%d %H:%M"),
            ("datetime", "%Y/%m/%dT%H:%M:%S"),
            ("datetime", "%Y/%m/%dT%H:%M:%S.000Z"),
            ("datetime", "%Y/%m/%dT%H:%M:%S.000"),
            ("datetime", "%Y/%m/%dT%H:%M:%S.%fZ"),
            ("datetime", "%Y/%m/%dT%H:%M:%S.%f%z"),
            ("datetime", "%Y/%m/%dT%H:%M:%S.%f"),
            ("datetime", "%Y/%m/%dT%H:%M:%SZ"),
            ("datetime", "%Y-%m-%dT%H:%M:%S.000Z"),
            ("datetime", "%Y-%m-%dT%H:%M:%S.000"),
            ("datetime", "%Y-%m-%dT%H:%M:%S.%fZ"),
            ("datetime", "%Y-%m-%dT%H:%M:%S.%f%z"),
            ("datetime", "%Y-%m-%dT%H:%M:%S.%f"),
            ("datetime", "%Y-%m-%dT%H:%M:%SZ"),
            ("datetime", "%Y-%m-%dT%H:%M:%S"),
            ("datetime", "%Y-%m-%d %H:%M:%S"),
            ("date", "%Y/%m/%d"),
            ("date", "%Y %m %d"),
            ("date", "%Y.%m.%d"),
            ("date", "%Y-%d-%m"),
            ("date", "%Y-%m"),
            ("date", "%Y.%m"),
            ("date", "%Y/%m"),
            ("date", "%Y %m"),
            ("date", "%Y"),
            ("date", "%Y.0"),
            ("datetime", "%d/%m/%Y %H:%M:%S"),
            ("datetime", "%d/%m/%Y %H:%M"),
            ("date", "%d-%m-%Y"),
            ("date", "%d-%m-%y"),
            ("date", "%d.%m.%Y"),
            ("date", "%d.%m.%y"),
            ("date", "%d/%m/%Y"),
            ("date", "%d/%m/%y"),
            ("date", "%d-%b-%Y"),
            ("date", "%d-%b-%y"),
            ("date", "%d %B %Y"),
            ("date", "%b %d, %Y"),
            ("date", "%b %d, %y"),
            ("date", "%b-%y"),
            ("date", "%B %Y"),
            ("date", "%m/%d/%Y"),
            ("datetime", "%s"),
        ]

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

        Fully vectorised: builds normalised lookup keys with ``str.to_lowercase``
        + ``str.replace_all``, then resolves via ``replace_strict``.
        Matching is case-insensitive and treats spaces as interchangeable with
        hyphens (legacy parity).  Values not found in the allowed list are left
        unchanged.

        Normalize categorical fields by replacing spaces and validating against allowed values.

        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names

        Returns:
            pl.LazyFrame: LazyFrame with normalised categorical fields
        """
        exprs = []
        for field, valid_values in self.valid_category_values.items():
            if field not in existing_columns:
                continue
            value_map = {v.lower().replace(" ", "-"): v for v in valid_values}
            if not value_map:
                continue

            # Normalised key: lowercase + spaces→hyphens
            normalized = (
                pl.col(field).cast(pl.Utf8).str.replace_all(" ", "-").str.to_lowercase()
            )
            # Look up canonical value; null when key not in map
            looked_up = normalized.replace_strict(
                value_map, default=None, return_dtype=pl.Utf8
            )
            exprs.append(
                pl.when(
                    pl.col(field).is_null()
                    | (
                        pl.col(field).cast(pl.Utf8).str.strip_chars().str.len_chars()
                        == 0
                    )
                )
                .then(pl.col(field))
                .when(looked_up.is_not_null())
                .then(looked_up)
                .otherwise(pl.col(field))
                .alias(field)
            )

        if not exprs:
            return lf
        return lf.with_columns(exprs)

    # -- Native Polars expression builders for common datatypes ----------------
    # These replace per-row Python callbacks (map_elements) with fully
    # vectorised Polars operations, eliminating Python overhead entirely.

    @staticmethod
    def _null_to_empty_expr(field: str) -> pl.Expr:
        """Cast to Utf8; replace null / blank with empty string.

        Used for identity datatypes such as ``curie`` where the legacy
        normaliser (``DataType.normalise``) returns the value unchanged.
        """
        return (
            pl.when(
                pl.col(field).is_null()
                | (pl.col(field).cast(pl.Utf8).str.strip_chars().str.len_chars() == 0)
            )
            .then(pl.lit(""))
            .otherwise(pl.col(field).cast(pl.Utf8))
            .alias(field)
        )

    @staticmethod
    def _string_normalise_expr(field: str) -> pl.Expr:
        """Native Polars equivalent of ``StringDataType.normalise()``.

        Replicates: strip → collapse whitespace → remove double-quote
        characters (both ASCII ``"`` and Unicode left-quote ``\u201c``).
        Null and blank values become empty strings.
        """
        return (
            pl.when(
                pl.col(field).is_null()
                | (pl.col(field).cast(pl.Utf8).str.strip_chars().str.len_chars() == 0)
            )
            .then(pl.lit(""))
            .otherwise(
                pl.col(field)
                .cast(pl.Utf8)
                .str.replace_all(r"\s+", " ")  # collapse whitespace runs
                .str.strip_chars()  # trim leading/trailing
                .str.replace_all('"', "", literal=True)  # ASCII double-quote
                .str.replace_all("\u201c", "", literal=True)  # left curly quote
            )
            .alias(field)
        )

    def _build_datetime_expr(
        self, field: str, far_past_date: date, far_future_date: date
    ) -> pl.Expr:
        """Vectorised Polars expression for one datetime field.

        Tries each format in ``_DATETIME_FORMATS`` via ``strptime(strict=False)``;
        ``pl.coalesce`` picks the first successful parse.  Far-past / far-future
        bounds are applied as vectorised ``pl.when`` guards.  Null, blank, and
        unparseable values become empty strings.
        """
        col = (
            pl.col(field).cast(pl.Utf8).str.strip_chars().str.strip_chars('",')
        )  # noqa: E501

        date_exprs: list[pl.Expr] = []
        for kind, fmt in self._DATETIME_FORMATS:
            if kind == "date":
                date_exprs.append(col.str.strptime(pl.Date, fmt, strict=False))
            else:
                date_exprs.append(
                    col.str.strptime(pl.Datetime, fmt, strict=False).dt.date()
                )

        parsed_str: pl.Expr = pl.coalesce(date_exprs).cast(pl.Utf8)

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

        return (
            pl.when(col.is_null() | (col.str.len_chars() == 0))
            .then(pl.lit(""))
            .otherwise(parsed_str.fill_null(pl.lit("")))
            .alias(field)
        )

    @staticmethod
    def _make_normaliser(dt, fname):
        """Build a normaliser closure wrapping a legacy datatype instance.

        Used for datatypes that cannot (yet) be expressed as native Polars
        expressions (e.g. datetime with 30+ format patterns).  The closure is
        applied via batch list comprehension rather than ``map_elements``.
        """
        issues = _NoOpIssues(fname)

        def _normalise(value):
            if value is None or (isinstance(value, str) and not value.strip()):
                return ""
            try:
                result = dt.normalise(str(value), issues=issues)
                return result if result is not None else ""
            except Exception as e:
                logger.debug("harmonise error for %s: %s", fname, e)
                return ""

        return _normalise

    @staticmethod
    def _make_datetime_fast_normaliser(far_past_date, far_future_date, fname):
        """Build a specialised datetime normaliser with an ISO-8601 fast-path.

        The vast majority of date values in production data are already in
        ``YYYY-MM-DD`` format.  For these, a simple regex match + string
        comparison (ISO dates sort lexicographically) replaces the expensive
        ``datetime.strptime()`` call, giving a ~5× speedup per value.

        Non-ISO values fall through to the full legacy normaliser which tries
        30+ ``strptime`` patterns.
        """
        import re
        from digital_land.datatype.factory import datatype_factory

        dt = datatype_factory(
            datatype_name="datetime",
            far_past_date=far_past_date,
            far_future_date=far_future_date,
        )
        issues = _NoOpIssues(fname)

        # Pre-compile the ISO pattern and pre-compute ISO bound strings.
        _iso_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        _far_past_iso = far_past_date.isoformat() if far_past_date else None
        _far_future_iso = far_future_date.isoformat() if far_future_date else None

        def _normalise(value):
            if value is None or (isinstance(value, str) and not value.strip()):
                return ""
            v = str(value).strip().strip('",')  # match legacy pre-processing

            # Fast path: ISO dates (~5× faster than strptime).
            if _iso_re.match(v):
                if _far_past_iso and v < _far_past_iso:
                    return ""
                if _far_future_iso and v > _far_future_iso:
                    return ""
                return v

            # Slow path: full legacy normaliser for non-ISO formats.
            try:
                result = dt.normalise(str(value), issues=issues)
                return result if result is not None else ""
            except Exception as e:
                logger.debug("harmonise error for %s: %s", fname, e)
                return ""

        return _normalise

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

        # Fields handled by native Polars expressions (fully vectorised).
        native_exprs: list[pl.Expr] = []
        # Fields requiring legacy normaliser via batch list comprehension.
        batch_normalisers = []

        for field in existing_columns:
            if field not in self.field_datatype_map:
                continue

            datatype_name = self.field_datatype_map[field]

            # -- Native Polars fast-paths (no Python per-row overhead) --
            if datatype_name == "curie":
                # DataType.normalise() is an identity function; just handle
                # null/blank → "".
                native_exprs.append(self._null_to_empty_expr(field))
                continue
            if datatype_name in ("string", "text"):
                native_exprs.append(self._string_normalise_expr(field))
                continue

            # -- Build normaliser for remaining types --
            if datatype_name == "datetime":
                far_past_date = date(1799, 12, 31)
                far_future_date = self._get_far_future_date(FAR_FUTURE_YEARS_AHEAD)
                native_exprs.append(
                    self._build_datetime_expr(field, far_past_date, far_future_date)
                )
                continue

            # Closure factory gives each column a stable datatype instance and
            # field-specific issues context.
            def _make_normaliser(dt, fname):
                issues = _NoOpIssues(fname)

                def _normalise(value):
                    if value is None or (isinstance(value, str) and not value.strip()):
                        return ""
                    try:
                        result = dt.normalise(str(value), issues=issues)
                        return result if result is not None else ""
                    except Exception as e:
                        logger.debug("harmonise error for %s: %s", fname, e)
                        return ""

                return _normalise

            datatype = datatype_factory(datatype_name)
            normaliser = _make_normaliser(datatype, field)

            # Spatial fields are batched through DuckDB for CRS reprojection.
            if datatype_name == "multipolygon":
                spatial_geometry_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue
            if datatype_name == "point":
                spatial_point_fields.append(field)
                spatial_normalisers[field] = normaliser
                continue

            batch_normalisers.append((field, normaliser))

        # 1) Apply native vectorised expressions (no collect needed).
        if native_exprs:
            lf = lf.with_columns(native_exprs)

        # 2) Batch-process remaining non-spatial fields in a single collect
        #    pass.  List comprehension is far faster than map_elements because
        #    it avoids per-element Polars↔Python serialisation overhead and
        #    requires only one collect instead of one per column.
        if batch_normalisers:
            df = lf.collect()
            updates = []
            for field, normaliser in batch_normalisers:
                values = df.get_column(field).cast(pl.Utf8).to_list()
                updates.append(
                    pl.Series(
                        field,
                        [normaliser(v) for v in values],
                        dtype=pl.Utf8,
                    )
                )
            lf = df.with_columns(updates).lazy()

        # 3) Spatial fields via DuckDB + legacy canonicalisation.
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
        """Canonicalise geometries with Shapely 2.x vectorised batch API.

        Steps mirror ``WktDataType.normalise`` / ``normalise_geometry``:
        precision round-trip → simplify → set_precision → make_valid →
        ensure MultiPolygon → orient rings → dump WKT.
        CRS reprojection is already handled by DuckDB upstream.
        """
        if not normalisers:
            return lf

        import shapely as _shp
        import numpy as np
        from shapely.geometry import MultiPolygon as _MP
        from shapely.geometry.polygon import orient as _orient

        df = lf.collect()
        updates: list[pl.Series] = []

        for field in normalisers:
            raw = df.get_column(field).to_list()
            wkt_arr = np.array(
                [v if (v and str(v).strip()) else None for v in raw],
                dtype=object,
            )

            # 1. Vectorised parse
            geoms = _shp.from_wkt(wkt_arr)
            valid_mask = ~_shp.is_missing(geoms)

            if not valid_mask.any():
                updates.append(pl.Series(field, [""] * len(raw), dtype=pl.Utf8))
                continue

            # 2. Precision round-trip (6 dp)
            wkt_6dp = _shp.to_wkt(
                geoms[valid_mask], rounding_precision=6, output_dimension=2
            )
            geoms[valid_mask] = _shp.from_wkt(wkt_6dp)

            # 3. Simplify
            simplified = _shp.simplify(geoms, 0.000005)
            was_valid = _shp.is_valid(geoms)
            simp_valid = _shp.is_valid(simplified)
            use_simp = (~was_valid | simp_valid) & valid_mask
            geoms = np.where(use_simp, simplified, geoms)

            # 4. Set precision
            geoms[valid_mask] = _shp.set_precision(
                geoms[valid_mask], 0.000001, mode="pointwise"
            )

            # 5. make_valid
            bad = ~_shp.is_valid(geoms) & valid_mask
            if bad.any():
                geoms[bad] = _shp.make_valid(geoms[bad])

            # 6. MultiPolygon + orient + buffer fix
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
                        p
                        for sub in g.geoms
                        if sub.geom_type in ("Polygon", "MultiPolygon")
                        for p in (
                            sub.geoms if sub.geom_type == "MultiPolygon" else [sub]
                        )
                    ]
                    g = _MP(polys) if polys else None
                elif gt != 6:  # not already MultiPolygon
                    geoms[i] = None
                    continue
                if g is not None and not g.is_valid:
                    g = g.buffer(0)
                    if g.geom_type == "Polygon":
                        g = _MP([g])
                    elif g.geom_type != "MultiPolygon":
                        geoms[i] = None
                        continue
                if g is not None:
                    g = _MP([_orient(poly) for poly in g.geoms])
                geoms[i] = g

            # 7. Dump WKT – matching legacy comma formatting
            wkt_out = _shp.to_wkt(geoms, rounding_precision=6, output_dimension=2)
            result = ["" if w is None else w.replace(", ", ",") for w in wkt_out]
            updates.append(pl.Series(field, result, dtype=pl.Utf8))

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

        # Vectorised CRS classification: extract the first two numbers from
        # each WKT value using Polars regex, then apply threshold-based SRID /
        # flip detection entirely in Polars expressions.
        helper_cols = ["__dl_idx"]

        for field in geometry_fields + point_fields:
            srid_col = f"__dl_srid_{field}"
            flip_col = f"__dl_flip_{field}"
            x_tmp = f"__dl_x_{field}"
            y_tmp = f"__dl_y_{field}"
            helper_cols.extend([srid_col, flip_col])

            # Extract first two numeric tokens (vectorised regex in Rust).
            nums = (
                pl.col(field)
                .cast(pl.Utf8)
                .str.extract_all(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
            )
            df = df.with_columns(
                nums.list.get(0, null_on_oob=True)
                .cast(pl.Float64, strict=False)
                .alias(x_tmp),
                nums.list.get(1, null_on_oob=True)
                .cast(pl.Float64, strict=False)
                .alias(y_tmp),
            )

            x = pl.col(x_tmp)
            y = pl.col(y_tmp)
            has = x.is_not_null() & y.is_not_null()

            is_deg = has & (x > -60) & (x < 60) & (y > -60) & (y < 60)
            is_en = (
                has
                & ~is_deg
                & (x > 1000)
                & (x < 1_000_000)
                & (y > 1000)
                & (y < 1_000_000)
            )
            is_m = has & ~is_deg & ~is_en & (y > 6_000_000) & (y < 10_000_000)
            is_mf = has & ~is_deg & ~is_en & ~is_m & (x > 6_000_000) & (x < 10_000_000)

            df = df.with_columns(
                pl.when(is_deg)
                .then(pl.lit("4326"))
                .when(is_en)
                .then(pl.lit("27700"))
                .when(is_m | is_mf)
                .then(pl.lit("3857"))
                .otherwise(pl.lit(""))
                .alias(srid_col),
                pl.when(is_mf)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias(flip_col),
            ).drop(x_tmp, y_tmp)

        con = self._duckdb_spatial_connection()
        con.register("dl_spatial", df.to_arrow())

        try:
            # Start with all non-helper columns quoted; replace spatial field
            # expressions in-place below to preserve column ordering.
            select_parts = [
                f'"{column}"' for column in df.columns if column not in helper_cols
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
                    f'END AS "{field}"'
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
                    f'END AS "{field}"'
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

        # Vectorised CRS classification for (GeoX, GeoY) pairs.
        x = (
            pl.col("GeoX")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
        )
        y = (
            pl.col("GeoY")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
        )
        has = x.is_not_null() & y.is_not_null()

        is_deg = has & (x > -60) & (x < 60) & (y > -60) & (y < 60)
        is_en = (
            has & ~is_deg & (x > 1000) & (x < 1_000_000) & (y > 1000) & (y < 1_000_000)
        )
        is_m = has & ~is_deg & ~is_en & (y > 6_000_000) & (y < 10_000_000)
        is_mf = has & ~is_deg & ~is_en & ~is_m & (x > 6_000_000) & (x < 10_000_000)

        df = df.with_columns(
            pl.when(is_deg)
            .then(pl.lit("4326"))
            .when(is_en)
            .then(pl.lit("27700"))
            .when(is_m | is_mf)
            .then(pl.lit("3857"))
            .otherwise(pl.lit(""))
            .alias("__dl_point_srid"),
            pl.when(is_mf)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("__dl_point_flip"),
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
                'THEN ST_Point(TRY_CAST("GeoX" AS DOUBLE), TRY_CAST("GeoY" AS DOUBLE)) '
                "WHEN __dl_point_srid = '4326' AND __dl_point_flip = TRUE "
                'THEN ST_Point(TRY_CAST("GeoY" AS DOUBLE), TRY_CAST("GeoX" AS DOUBLE)) '
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
                'WHEN "GeoX" IS NULL OR "GeoY" IS NULL OR trim(CAST("GeoX" AS VARCHAR)) = \'\' OR trim(CAST("GeoY" AS VARCHAR)) = \'\' OR __dl_point_srid = \'\' '
                "THEN '' "
                f"ELSE coalesce(CAST(round(ST_X({point_case}), 6) AS VARCHAR), '') END AS \"GeoX\", "
                "CASE "
                'WHEN "GeoX" IS NULL OR "GeoY" IS NULL OR trim(CAST("GeoX" AS VARCHAR)) = \'\' OR trim(CAST("GeoY" AS VARCHAR)) = \'\' OR __dl_point_srid = \'\' '
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
            f'WHEN "{srid_col}" = \'4326\' AND "{flip_col}" = FALSE THEN {geom} '
            f'WHEN "{srid_col}" = \'4326\' AND "{flip_col}" = TRUE THEN ST_FlipCoordinates({geom}) '
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

        Issue logging is currently a no-op (``_NoOpIssues``).  To avoid the
        cost of collecting the frame and iterating every row for no effect,
        the check is skipped until real issue logging is wired in.
        """
        # TODO: restore row-level checks once a real IssueLog is available.
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
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)
