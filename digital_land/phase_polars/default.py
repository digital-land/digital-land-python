import polars as pl

from .phase import PolarsPhase


class DefaultPhase(PolarsPhase):
    """
    Apply default field values and default field-to-field mappings.
    """

    def __init__(self, issues=None, default_fields=None, default_values=None):
        if default_fields is None:
            default_fields = {}
        if default_values is None:
            default_values = {}
        self.issues = issues
        self.default_values = default_values
        self.default_fields = default_fields

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        # Apply default_fields: if field is empty, copy from another field
        for field, default_field in self.default_fields.items():
            if default_field not in df.columns:
                continue
            if field not in df.columns:
                df = df.with_columns(pl.lit("").alias(field))

            df = df.with_columns(
                pl.when(
                    pl.col(field).is_null()
                    | (pl.col(field) == "")
                )
                .then(
                    pl.when(
                        pl.col(default_field).is_not_null()
                        & (pl.col(default_field) != "")
                    )
                    .then(pl.col(default_field))
                    .otherwise(pl.col(field))
                )
                .otherwise(pl.col(field))
                .alias(field)
            )

        # Apply default_values: if field is empty, use a fixed default value
        for field, value in self.default_values.items():
            if not value:
                continue

            if field not in df.columns:
                df = df.with_columns(pl.lit("").alias(field))

            df = df.with_columns(
                pl.when(
                    pl.col(field).is_null()
                    | (pl.col(field) == "")
                )
                .then(pl.lit(value))
                .otherwise(pl.col(field))
                .alias(field)
            )

        return df
