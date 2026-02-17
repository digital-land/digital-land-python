import polars as pl

from .phase import PolarsPhase


class LoadPhase(PolarsPhase):
    """
    Load a CSV file into a Polars DataFrame.
    """

    def __init__(self, path=None, resource=None, dataset=None):
        self.path = path
        self.resource = resource
        self.dataset = dataset

    def process(self, df=None):
        from pathlib import Path

        path = self.path
        resource = self.resource or (Path(path).stem if path else None)

        result = pl.read_csv(
            str(path),
            infer_schema_length=0,
            null_values=[""],
            truncate_ragged_lines=True,
            ignore_errors=True,
        )
        result = result.with_columns(pl.all().cast(pl.Utf8).fill_null(""))

        n = result.height
        result = result.with_columns(
            pl.lit(resource or "").alias("__resource"),
            pl.arange(2, n + 2).alias("__line_number"),
            pl.arange(1, n + 1).alias("__entry_number"),
            pl.lit(str(path) if path else "").alias("__path"),
        )

        return result
