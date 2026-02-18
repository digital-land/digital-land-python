import polars as pl
from typing import Dict, List, Any, Iterator
import io


class StreamToPolarsConverter:
    """Utility class to convert dictionary objects to Polars LazyFrame objects."""
    
    @staticmethod
    def from_stream(stream: Iterator[Dict[str, Any]]) -> pl.LazyFrame:
        """
        Convert a Stream object (from convert phase) to a Polars LazyFrame.
        
        Args:
            stream: Iterator yielding blocks with 'line' or 'row' keys
            
        Returns:
            pl.LazyFrame: Polars LazyFrame object with inferred schema
        """
        blocks = list(stream)
        if not blocks:
            return pl.DataFrame().lazy()
        
        fieldnames = blocks[0].get("line", [])
        
        # Build CSV string for Polars to parse with type inference
        csv_lines = [','.join(f'"{field}"' for field in fieldnames)]
        
        for block in blocks[1:]:
            if "row" in block and block["row"]:
                row = [str(block["row"].get(field, '')) for field in fieldnames]
            elif "line" in block:
                row = [str(val) for val in block["line"]]
            else:
                continue
            csv_lines.append(','.join(f'"{val}"' for val in row))
        
        if len(csv_lines) <= 1:
            return pl.DataFrame().lazy()
        
        # Use Polars CSV reader with type inference
        csv_string = '\n'.join(csv_lines)
        return pl.read_csv(io.StringIO(csv_string), try_parse_dates=True).lazy()
