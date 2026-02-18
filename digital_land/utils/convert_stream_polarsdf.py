import polars as pl
from typing import Dict, List, Any, Iterator


class StreamToPolarsConverter:
    """Utility class to convert dictionary objects to Polars LazyFrame objects."""
    
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
