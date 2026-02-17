import polars as pl

from .phase import PolarsPhase


class MigratePhase(PolarsPhase):
    """
    Rename fields to match the latest specification.
    """

    def __init__(self, fields, migrations):
        self.migrations = migrations
        self.fields = list(
            set(fields + ["entity", "organisation", "prefix", "reference"])
        )

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        meta_cols = [c for c in df.columns if c.startswith("__")]
        data_cols = [c for c in df.columns if not c.startswith("__")]

        exprs = []
        for field in self.fields:
            migrated_from = self.migrations.get(field)
            if migrated_from and migrated_from in df.columns:
                exprs.append(pl.col(migrated_from).alias(field))
            elif field in df.columns:
                exprs.append(pl.col(field))
            # else: field not present in df, skip

        # Handle GeoX/GeoY → point conversion
        has_geoxy = "GeoX" in df.columns and "GeoY" in df.columns
        if has_geoxy and "point" in self.fields:
            exprs.append(
                pl.when(
                    pl.col("GeoX").is_not_null()
                    & (pl.col("GeoX") != "")
                    & pl.col("GeoY").is_not_null()
                    & (pl.col("GeoY") != "")
                )
                .then(
                    pl.concat_str(
                        [pl.lit("POINT("), pl.col("GeoX"), pl.lit(" "), pl.col("GeoY"), pl.lit(")")],
                        separator="",
                    )
                )
                .otherwise(pl.lit(""))
                .alias("point")
            )

        # Add metadata columns
        for mc in meta_cols:
            exprs.append(pl.col(mc))

        # Deduplicate by alias (keep last in case of conflict, e.g. point)
        seen = {}
        for expr in exprs:
            name = expr.meta.output_name()
            seen[name] = expr
        exprs = list(seen.values())

        return df.select(exprs)
