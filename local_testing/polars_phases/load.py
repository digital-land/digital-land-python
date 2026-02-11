"""Phase 1: LoadPhase - Read Parquet into DataFrame.

This module provides the PolarsLoadPhase class, which is responsible for
loading data from Parquet files into Polars DataFrames. This is the
first phase in the Polars-based pipeline after file conversion has been
completed.

Note: This phase only reads Parquet files for optimal performance and type
preservation. Input files must be converted to Parquet format first using
GMLConverter or appropriate converter.
"""

import polars as pl
from pathlib import Path
from .base import PolarsPhase


class PolarsLoadPhase(PolarsPhase):
    """Load Parquet files into a Polars DataFrame.

    This phase reads data from disk into memory as a Polars DataFrame.
    Only Parquet format is supported for optimal performance and type
    preservation. Input files must be converted to Parquet before this phase.

    The phase also ensures required metadata columns exist in the DataFrame:
    - priority: Used for record priority sorting (default: 1)
    - resource: Source resource identifier (default: "")
    - line-number: Original line number from source (default: 0)

    Attributes:
        name (str): Phase name identifier ("LoadPhase")
        path (Path): Path to the input Parquet file

    Examples:
        >>> # Load a Parquet file
        >>> phase = PolarsLoadPhase("data/input.parquet")
        >>> df = phase.process()

        >>> # Use in a pipeline
        >>> from polars_phases import run_polars_pipeline
        >>> metrics, harmonised_count, facts_count = run_polars_pipeline(
        ...     input_csv=Path("input.parquet"),
        ...     # ... other parameters
        ... )
    """

    name = "LoadPhase"

    def __init__(self, path: str):
        """Initialize the LoadPhase with a Parquet file path.

        Args:
            path (str): Path to the input Parquet file. Can be
                absolute or relative. Must have .parquet extension.

        Raises:
            FileNotFoundError: If the file does not exist (raised during process())
            ValueError: If the file is not a Parquet file (raised during process())
        """
        self.path = Path(path)

    def process(self, df: pl.DataFrame = None) -> pl.DataFrame:
        """Read Parquet file into DataFrame.

        The method performs the following operations:
        1. Validates file has .parquet extension
        2. Reads Parquet file using Polars (fast binary format with type preservation)
        3. Adds required metadata columns if missing

        Args:
            df (pl.DataFrame, optional): Input DataFrame. Ignored in this phase
                as data is loaded from disk. Included for pipeline compatibility.

        Returns:
            pl.DataFrame: Loaded DataFrame with the following guarantees:
                - All columns from the source file
                - "priority" column (Int64, default: 1)
                - "resource" column (String, default: "")
                - "line-number" column (Int64, default: 0)

        Raises:
            FileNotFoundError: If the specified file does not exist
            ValueError: If file is not a Parquet file
            pl.exceptions.ComputeError: If file cannot be parsed
            PermissionError: If file cannot be read due to permissions

        Performance Notes:
            - Parquet provides 3-10x faster reads than CSV
            - Type information is preserved from Parquet metadata
            - No type inference overhead

        Examples:
            >>> phase = PolarsLoadPhase("data.parquet")
            >>> df = phase.process()
            >>> print(df.shape)
            (10000, 15)

            >>> # Verify metadata columns exist
            >>> assert "priority" in df.columns
            >>> assert "resource" in df.columns
            >>> assert "line-number" in df.columns
        """
        # Validate Parquet file
        if self.path.suffix.lower() != ".parquet":
            raise ValueError(
                f"LoadPhase only supports Parquet files. Got: {self.path.suffix}. "
                f"Please convert to Parquet first."
            )

        # Read Parquet file
        df = pl.read_parquet(self.path)

        # Add metadata columns if they don't exist (needed for facts table)
        if "priority" not in df.columns:
            df = df.with_columns(pl.lit(1).alias("priority"))
        if "resource" not in df.columns:
            df = df.with_columns(pl.lit("").alias("resource"))
        if "line-number" not in df.columns:
            # Will be set properly when entry-number is added in ParsePhase
            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias("line-number"))

        return df
