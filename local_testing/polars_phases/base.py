"""Base class for Polars pipeline phases."""

import polars as pl


class PolarsPhase:
    """Base class for Polars-based pipeline phases."""

    name: str = "PolarsPhase"

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process a DataFrame and return the transformed DataFrame."""
        return df
