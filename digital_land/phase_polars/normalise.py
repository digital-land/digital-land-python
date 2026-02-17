import csv
import os
import re

import polars as pl

from .phase import PolarsPhase

patch_dir = os.path.join(os.path.dirname(__file__), "../patch")


class NormalisePhase(PolarsPhase):
    """
    Normalise CSV whitespace, strip null patterns and skip matching rows.

    In the streaming pipeline this operates on raw lines *before* parsing.
    In the Polars pipeline it operates on already-parsed string columns
    which gives equivalent results.
    """

    null_path = os.path.join(patch_dir, "null.csv")

    def __init__(self, skip_patterns=None):
        if skip_patterns is None:
            skip_patterns = []
        self.skip_patterns = [re.compile(p) for p in skip_patterns]

        self.null_patterns = []
        if os.path.exists(self.null_path):
            for row in csv.DictReader(open(self.null_path, newline="")):
                self.null_patterns.append(re.compile(row["pattern"]))

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        # Identify data columns (non-metadata)
        data_cols = [c for c in df.columns if not c.startswith("__")]

        # Strip whitespace from all data columns
        strip_exprs = [
            pl.col(c)
            .str.strip_chars()
            .str.replace_all(r"\r", "")
            .str.replace_all(r"\n", "\r\n")
            .alias(c)
            for c in data_cols
        ]
        if strip_exprs:
            df = df.with_columns(strip_exprs)

        # Apply null patterns to all data columns
        for pattern in self.null_patterns:
            null_exprs = [
                pl.col(c).str.replace_all(pattern.pattern, "").alias(c)
                for c in data_cols
            ]
            if null_exprs:
                df = df.with_columns(null_exprs)

        # Remove completely blank rows (all data columns empty or null)
        if data_cols:
            not_blank = pl.lit(False)
            for c in data_cols:
                not_blank = not_blank | (
                    pl.col(c).is_not_null() & (pl.col(c) != "")
                )
            df = df.filter(not_blank)

        # Skip rows matching skip patterns (matched against full comma-joined line)
        if self.skip_patterns and data_cols:
            concat_expr = pl.concat_str(
                [pl.col(c).fill_null("") for c in data_cols], separator=","
            ).alias("__skip_line")
            df = df.with_columns(concat_expr)

            for pattern in self.skip_patterns:
                df = df.filter(
                    ~pl.col("__skip_line").str.contains(pattern.pattern)
                )

            df = df.drop("__skip_line")

        return df
