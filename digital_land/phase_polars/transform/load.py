"""Phase 1: LoadPhase - Read CSV or Parquet into DataFrame.

This module provides the PolarsLoadPhase class, which is responsible for
loading data from CSV or Parquet files into Polars DataFrames. This is the
first phase in the Polars-based pipeline after file conversion has been
completed.

Note: This phase assumes the input file has already been converted to
CSV or Parquet format. For GML or other source formats, use GMLConverter
or the appropriate converter first.
"""

import polars as pl
from pathlib import Path
from .base import PolarsPhase


class PolarsLoadPhase(PolarsPhase):
    """Load CSV or Parquet files into a Polars DataFrame.
    
    This phase reads data from disk into memory as a Polars DataFrame.
    It handles both CSV and Parquet formats, with Parquet being preferred
    for performance and type preservation.
    
    The phase also ensures required metadata columns exist in the DataFrame:
    - priority: Used for record priority sorting (default: 1)
    - resource: Source resource identifier (default: "")
    - line-number: Original line number from source (default: 0)
    
    Attributes:
        name (str): Phase name identifier ("LoadPhase")
        path (Path): Path to the input CSV or Parquet file
    
    Examples:
        >>> # Load a Parquet file
        >>> phase = PolarsLoadPhase("data/input.parquet")
        >>> df = phase.process()
        
        >>> # Load a CSV file
        >>> phase = PolarsLoadPhase("data/input.csv")
        >>> df = phase.process()
        
        >>> # Use in a pipeline
        >>> from polars_phases import run_polars_pipeline
        >>> metrics, harmonised_count, facts_count = run_polars_pipeline(
        ...     input_csv=Path("input.csv"),
        ...     # ... other parameters
        ... )
    """

    name = "LoadPhase"

    def __init__(self, path: str):
        """Initialize the LoadPhase with a file path.
        
        Args:
            path (str): Path to the input CSV or Parquet file. Can be
                absolute or relative. The file extension determines the
                read method (.parquet or .csv).
        
        Raises:
            FileNotFoundError: If the file does not exist (raised during process())
            ValueError: If the file format is not supported (raised during process())
        """
        self.path = Path(path)

    def process(self, df: pl.DataFrame = None) -> pl.DataFrame:
        """Read CSV or Parquet file into DataFrame based on file extension.
        
        The method performs the following operations:
        1. Determines file format from extension
        2. Reads file using appropriate Polars method:
           - Parquet: Fast binary format with type preservation
           - CSV: Text format with type inference
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
            pl.exceptions.ComputeError: If file cannot be parsed
            PermissionError: If file cannot be read due to permissions
        
        Performance Notes:
            - Parquet is 3-10x faster to read than CSV
            - CSV type inference scans up to 10,000 rows
            - For large CSVs, consider converting to Parquet first
        
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
        if self.path.suffix.lower() == ".parquet":
            # Parquet is much faster to read and preserves types
            df = pl.read_parquet(self.path)
        else:
            # CSV fallback with type inference
            df = pl.read_csv(
                self.path,
                infer_schema_length=10000,  # Scan first 10k rows for types
                null_values=["", "NULL", "null", "None", "none"],
                try_parse_dates=False,  # We'll handle dates in later phases
            )
        
        # Add metadata columns if they don't exist (needed for facts table)
        if "priority" not in df.columns:
            df = df.with_columns(pl.lit(1).alias("priority"))
        if "resource" not in df.columns:
            df = df.with_columns(pl.lit("").alias("resource"))
        if "line-number" not in df.columns:
            # Will be set properly when entry-number is added in ParsePhase
            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias("line-number"))
        
        return df
