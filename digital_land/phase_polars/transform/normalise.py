import os
import re
import csv
import polars as pl
from typing import List


patch_dir = os.path.join(os.path.dirname(__file__), "../../patch")


class NormalisePhase:
    """Normalise CSV data using Polars LazyFrame operations."""
    
    spaces = " \n\r\t\f"
    null_patterns: List[re.Pattern] = []
    skip_patterns: List[re.Pattern] = []
    null_path = os.path.join(patch_dir, "null.csv")

    def __init__(self, skip_patterns=[]):
        self.skip_patterns = []
        for pattern in skip_patterns:
            self.skip_patterns.append(re.compile(pattern))

        for row in csv.DictReader(open(self.null_path, newline="")):
            self.null_patterns.append(re.compile(row["pattern"]))

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Process a Polars LazyFrame to normalise whitespace and strip nulls.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: Normalised LazyFrame
        """
        # Get all string columns
        string_cols = lf.collect_schema().names()
        
        # Normalise whitespace: strip spaces and replace line breaks
        for col in string_cols:
            lf = lf.with_columns(
                pl.col(col)
                .cast(pl.Utf8)
                .str.strip_chars(self.spaces)
                .str.replace_all("\r", "")
                .str.replace_all("\n", "\r\n")
                .alias(col)
            )
        
        # Strip nulls using regex patterns
        for pattern in self.null_patterns:
            for col in string_cols:
                lf = lf.with_columns(
                    pl.col(col).str.replace_all(pattern.pattern, "").alias(col)
                )
        
        # Filter out blank rows (all columns empty)
        filter_expr = pl.lit(False)
        for col in string_cols:
            filter_expr = filter_expr | (pl.col(col).str.len_chars() > 0)
        
        lf = lf.filter(filter_expr)
        
        # Apply skip patterns if any
        if self.skip_patterns:
            # Create concatenated line for pattern matching
            concat_expr = pl.concat_str([pl.col(c) for c in string_cols], separator=",")
            
            for pattern in self.skip_patterns:
                lf = lf.filter(~concat_expr.str.contains(pattern.pattern))
        
        return lf
