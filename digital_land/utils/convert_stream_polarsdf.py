import polars as pl
from typing import Dict, List, Any, Union, Iterator


class DictToPolarsConverter:
    """Utility class to convert dictionary objects to Polars LazyFrame objects."""

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> pl.LazyFrame:
        """
        Convert a dictionary to a Polars LazyFrame.
        
        Args:
            data: Dictionary with column names as keys and lists of values
            
        Returns:
            pl.LazyFrame: Polars LazyFrame object
        """
        return pl.DataFrame(data).lazy()

    @staticmethod
    def from_records(records: List[Dict[str, Any]]) -> pl.LazyFrame:
        """
        Convert a list of dictionaries (records) to a Polars LazyFrame.
        
        Args:
            records: List of dictionaries where each dict represents a row
            
        Returns:
            pl.LazyFrame: Polars LazyFrame object
        """
        return pl.DataFrame(records).lazy()

    @staticmethod
    def from_csv_dict(csv_dict: Dict[str, List[Any]]) -> pl.LazyFrame:
        """
        Convert CSV-like dictionary (columns and data) to Polars LazyFrame.
        
        Args:
            csv_dict: Dictionary with 'columns' and 'data' keys
            
        Returns:
            pl.LazyFrame: Polars LazyFrame object
        """
        if "columns" in csv_dict and "data" in csv_dict:
            return pl.DataFrame(csv_dict["data"], schema=csv_dict["columns"]).lazy()
        return DictToPolarsConverter.from_dict(csv_dict)

    @staticmethod
    def from_stream(stream: Iterator[Dict[str, Any]]) -> pl.LazyFrame:
        """
        Convert a Stream object (from convert phase) to a Polars LazyFrame.
        
        Args:
            stream: Iterator yielding blocks with 'line' or 'row' keys
            
        Returns:
            pl.LazyFrame: Polars LazyFrame object
        """
        blocks = list(stream)
        if not blocks:
            return pl.DataFrame().lazy()
        
        fieldnames = blocks[0].get("line", [])
        
        rows = []
        for block in blocks[1:]:
            if "row" in block and block["row"]:
                rows.append(block["row"])
            elif "line" in block:
                rows.append(dict(zip(fieldnames, block["line"])))
        
        return pl.DataFrame(rows).lazy()
