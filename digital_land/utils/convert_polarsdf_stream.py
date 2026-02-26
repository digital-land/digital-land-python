import polars as pl
from typing import Iterator, Dict, Any


def _stringify_value(value: Any) -> str:
    """Convert a value to string matching legacy pipeline conventions.

    - None/null → ""
    - Float with no fractional part → integer string (90.0 → "90")
    - Everything else → str()
    """
    if value is None:
        return ""
    if isinstance(value, float):
        # Match legacy: 90.0 → "90", but 90.5 → "90.5"
        if value == int(value) and not (value != value):  # guard against NaN
            return str(int(value))
        return str(value)
    return str(value)


def polars_to_stream(
    lf: pl.LazyFrame,
    dataset=None,
    resource=None,
    path=None,
    parsed=False,
) -> Iterator[Dict[str, Any]]:
    """
    Convert a Polars LazyFrame back to stream format.

    Values are stringified to match legacy pipeline conventions:
    nulls become "", whole floats drop the decimal (90.0 → "90").

    Args:
        lf: Polars LazyFrame object
        dataset: Dataset name
        resource: Resource name
        path: File path
        parsed: If True, output parsed format (with 'row' dict).
                If False, output unparsed format (with 'line' list).

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
                "row": {k: _stringify_value(v) for k, v in row_dict.items()},
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
                "line": [_stringify_value(v) for v in row_tuple],
                "line-number": line_number,
                "row": {},
            }
