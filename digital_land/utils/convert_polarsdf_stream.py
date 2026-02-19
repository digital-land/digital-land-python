import polars as pl
from typing import Iterator, Dict, Any


def polars_to_stream(lf: pl.LazyFrame, dataset=None, resource=None, path=None, parsed=False) -> Iterator[Dict[str, Any]]:
    """
    Convert a Polars LazyFrame back to stream format.
    
    Args:
        lf: Polars LazyFrame object
        dataset: Dataset name
        resource: Resource name
        path: File path
        parsed: If True, output parsed format (with 'row' dict). If False, output unparsed format (with 'line' list)
        
    Yields:
        Dict[str, Any]: Stream blocks
    """
    df = lf.collect()
    
    if parsed:
        for entry_number, row_dict in enumerate(df.to_dicts(), start=1):
            yield {
                "dataset": dataset,
                "path": path,
                "resource": resource,
                "entry-number": entry_number,
                "row": row_dict,
            }
    else:
        yield {
            "dataset": dataset,
            "path": path,
            "resource": resource,
            "line": df.columns,
            "line-number": 0,
            "row": {},
        }
        
        for line_number, row_tuple in enumerate(df.iter_rows(), start=1):
            yield {
                "dataset": dataset,
                "path": path,
                "resource": resource,
                "line": list(row_tuple),
                "line-number": line_number,
                "row": {},
            }
