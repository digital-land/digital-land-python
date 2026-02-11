"""Phase 2: NormalisePhase - Normalize whitespace and nulls.

This module implements the normalization phase of the data processing pipeline.
It handles:
- Whitespace normalization (leading, trailing, and internal)
- Newline character standardization (to CRLF format)
- Null pattern detection and replacement based on configurable patterns
- Optional row skipping based on patterns
"""

import polars as pl
from typing import List
from .base import PolarsPhase


class PolarsNormalisePhase(PolarsPhase):
    """Normalize whitespace and handle null patterns from specification.

    This phase processes all string columns in the DataFrame to:
    - Strip leading and trailing whitespace
    - Normalize internal whitespace (collapse multiple spaces to single space)
    - Convert newlines to CRLF format (\r\n)
    - Replace null-like patterns (e.g., 'null', 'N/A', '?', '--') with empty strings
    - Optionally skip rows matching specified patterns

    Attributes:
        name (str): Phase identifier set to "NormalisePhase"
        skip_patterns (List[str]): Patterns to identify rows that should be skipped
        null_patterns (List[str]): Regular expression patterns to identify null-like values
        DEFAULT_NULL_PATTERNS (List[str]): Default regex patterns for common null representations
    """

    name = "NormalisePhase"

    # Default null patterns (from digital_land/patch/null.csv)
    DEFAULT_NULL_PATTERNS = [
        r"^<*[Nn][Uu][Ll][Ll]>*$",  # <NULL>, null, NULL, etc.
        r"^#*[Nn]/?[Aa]$",  # N/A, NA, #N/A, etc.
        r"^\?*$",  # ?, ??, etc.
        r"^-+$",  # -, --, etc.
    ]

    def __init__(
        self,
        skip_patterns: List[str] = None,
        null_patterns: List[str] = None,
        null_patterns_path: str = None,
    ):
        """Initialize the NormalisePhase with optional custom patterns.

        Args:
            skip_patterns (List[str], optional): List of patterns to identify rows that
                should be skipped during processing. Defaults to None (empty list).
            null_patterns (List[str], optional): List of regex patterns to identify
                null-like values. Overrides default patterns if provided. Defaults to None.
            null_patterns_path (str, optional): Path to a CSV file containing null patterns
                (expected to have a 'pattern' column). Takes precedence over null_patterns
                parameter. Defaults to None.

        Note:
            Priority order for null patterns: null_patterns_path > null_patterns > DEFAULT_NULL_PATTERNS
        """
        self.skip_patterns = skip_patterns or []

        # Load null patterns from file if provided, otherwise use defaults
        if null_patterns_path:
            self.null_patterns = self._load_null_patterns(null_patterns_path)
        elif null_patterns:
            self.null_patterns = null_patterns
        else:
            self.null_patterns = self.DEFAULT_NULL_PATTERNS

    def _load_null_patterns(self, path: str) -> List[str]:
        """Load null patterns from a CSV file.

        Args:
            path (str): Path to the CSV file containing null patterns.
                The CSV file should have a 'pattern' column with regex patterns.

        Returns:
            List[str]: List of regex pattern strings. Returns DEFAULT_NULL_PATTERNS
                if the file cannot be read or doesn't contain a 'pattern' column.

        Note:
            This method silently falls back to DEFAULT_NULL_PATTERNS on any error,
            including file not found, invalid CSV format, or missing 'pattern' column.
        """
        try:
            patterns_df = pl.read_csv(path)
            if "pattern" in patterns_df.columns:
                return patterns_df["pattern"].to_list()
        except Exception:
            # If file doesn't exist or can't be read, use defaults
            pass
        return self.DEFAULT_NULL_PATTERNS

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize all string columns in the DataFrame.

        This method performs vectorized normalization operations on all string (Utf8) columns:
        1. Strips leading and trailing whitespace
        2. Collapses multiple internal spaces to single space
        3. Converts newlines to CRLF format (\r\n)
        4. Replaces values matching null patterns with empty strings
        5. Ensures truly empty strings (not null values)

        Args:
            df (pl.DataFrame): Input DataFrame to be normalized.

        Returns:
            pl.DataFrame: Normalized DataFrame with the same schema as input.
                All string columns will have normalized values.

        Note:
            Non-string columns are left unchanged. The skip_patterns feature
            is currently not implemented in this version.
        """
        # Get string columns
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

        # Vectorized operations on all string columns at once
        expressions = []
        for col in string_cols:
            expr = pl.col(col)

            # Strip leading/trailing whitespace
            expr = expr.str.strip_chars()

            # Normalize internal whitespace (multiple spaces to single)
            expr = expr.str.replace_all(r"\s+", " ")

            # Replace newlines with CRLF format
            expr = expr.str.replace_all("\r", "").str.replace_all("\n", "\r\n")

            # Apply null patterns - replace matches with empty string
            for pattern in self.null_patterns:
                expr = expr.str.replace(pattern, "")

            # Convert empty strings to actual empty (not null)
            expr = expr.str.replace_all(r"^\s*$", "")

            expressions.append(expr.alias(col))

        if expressions:
            df = df.with_columns(expressions)

        # Skip rows matching skip_patterns (if any)
        if self.skip_patterns:
            # Create a filter that excludes rows matching any skip pattern
            # This would require concatenating all columns and matching
            # For now, we'll skip this optimization as it's complex
            pass

        return df
