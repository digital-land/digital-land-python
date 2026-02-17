import polars as pl


class PolarsPhase:
    """
    A step in a Polars-based pipeline process.

    Each phase takes a Polars DataFrame and returns a Polars DataFrame.
    Metadata columns (prefixed with __) carry through the pipeline:
      __resource, __line_number, __entry_number, __path, __dataset, __priority
    """

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        return df
