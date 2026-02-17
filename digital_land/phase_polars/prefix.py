import polars as pl

from .phase import PolarsPhase


class EntityPrefixPhase(PolarsPhase):
    """
    Ensure every entry has a prefix field.
    """

    def __init__(self, dataset=None):
        self.dataset = dataset

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "prefix" not in df.columns:
            df = df.with_columns(pl.lit(self.dataset).alias("prefix"))
        else:
            df = df.with_columns(
                pl.when(
                    pl.col("prefix").is_null() | (pl.col("prefix") == "")
                )
                .then(pl.lit(self.dataset))
                .otherwise(pl.col("prefix"))
                .alias("prefix")
            )

        return df
