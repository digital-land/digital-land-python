import re

import polars as pl

from .phase import PolarsPhase


class FilterPhase(PolarsPhase):
    """
    Filter rows based on regex patterns applied to field values.
    Only rows where *all* filter patterns match are kept.
    """

    def __init__(self, filters=None):
        if filters is None:
            filters = {}
        self.filters = {}
        for field, pattern in filters.items():
            self.filters[field] = re.compile(pattern)

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0 or not self.filters:
            return df

        mask = pl.lit(True)
        for field, pattern in self.filters.items():
            if field in df.columns:
                mask = mask & pl.col(field).fill_null("").str.contains(
                    f"^(?:{pattern.pattern})"
                )

        return df.filter(mask)
