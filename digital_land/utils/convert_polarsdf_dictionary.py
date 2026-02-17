import polars as pl
from typing import Dict, List, Any, Iterator


class PolarsToDictConverter:
    """Utility class to convert Polars LazyFrame objects back to dictionary objects."""

    @staticmethod
    def to_dict(lf: pl.LazyFrame) -> Dict[str, List[Any]]:
        """
        Convert a Polars LazyFrame to a dictionary with column names as keys.
        
        Args:
            lf: Polars LazyFrame object
            
        Returns:
            Dict[str, List[Any]]: Dictionary with column names as keys and lists of values
        """
        df = lf.collect()
        return df.to_dict(as_series=False)

    @staticmethod
    def to_records(lf: pl.LazyFrame) -> List[Dict[str, Any]]:
        """
        Convert a Polars LazyFrame to a list of dictionaries (records).
        
        Args:
            lf: Polars LazyFrame object
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries where each dict represents a row
        """
        df = lf.collect()
        return df.to_dicts()

    @staticmethod
    def to_csv_dict(lf: pl.LazyFrame) -> Dict[str, Any]:
        """
        Convert a Polars LazyFrame to CSV-like dictionary with 'columns' and 'data' keys.
        
        Args:
            lf: Polars LazyFrame object
            
        Returns:
            Dict[str, Any]: Dictionary with 'columns' and 'data' keys
        """
        df = lf.collect()
        return {
            "columns": df.columns,
            "data": df.rows()
        }

    @staticmethod
    def to_stream_blocks(lf: pl.LazyFrame, dataset=None, resource=None, path=None) -> Iterator[Dict[str, Any]]:
        """
        Convert a Polars LazyFrame to stream blocks compatible with ParsePhase.
        
        Args:
            lf: Polars LazyFrame object
            dataset: Dataset name
            resource: Resource name
            path: File path
            
        Yields:
            Dict[str, Any]: Stream blocks with 'row', 'entry-number', etc.
        """
        df = lf.collect()
        for entry_number, row_dict in enumerate(df.to_dicts(), start=1):
            yield {
                "dataset": dataset,
                "path": path,
                "resource": resource,
                "line": list(row_dict.values()),
                "line-number": entry_number,
                "row": row_dict,
                "entry-number": entry_number,
            }
