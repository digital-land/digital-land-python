import csv
import logging

import polars as pl

from .phase import PolarsPhase


class SavePhase(PolarsPhase):
    """
    Save the DataFrame to a CSV file, then pass through.
    """

    def __init__(self, path=None, f=None, fieldnames=None, enabled=True):
        self.path = path
        self.f = f
        self.fieldnames = fieldnames
        self.enabled = enabled

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if not self.enabled or df is None or df.height == 0:
            return df

        # Select only data columns (non-metadata)
        data_cols = [c for c in df.columns if not c.startswith("__")]

        if self.fieldnames:
            # Only keep requested fieldnames that exist
            keep = sorted([f for f in self.fieldnames if f in data_cols])
        else:
            keep = sorted(data_cols)

        if not keep:
            return df

        out_df = df.select(keep)

        if self.f:
            # Write to file object
            csv_str = out_df.write_csv()
            self.f.write(csv_str)
        elif self.path:
            out_df.write_csv(str(self.path))

        return df
