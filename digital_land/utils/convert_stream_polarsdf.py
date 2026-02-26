import polars as pl
from typing import Dict, List, Any, Iterator
import io


class StreamToPolarsConverter:
    """Utility class to convert dictionary objects to Polars LazyFrame objects."""

    @staticmethod
    def from_stream(stream: Iterator[Dict[str, Any]]) -> pl.LazyFrame:
        """
        Convert a Stream object (from convert phase) to a Polars LazyFrame.

        Type inference is enabled for numeric columns to benefit from
        Polars' columnar performance.  Date parsing is DISABLED here
        because the legacy pipeline treats dates as strings until the
        harmonise phase applies UK-specific day-first parsing rules.
        Letting Polars auto-parse dates would apply month-first (US)
        conventions and produce different results.

        Args:
            stream: Iterator yielding blocks with 'line' or 'row' keys

        Returns:
            pl.LazyFrame: Polars LazyFrame object with inferred numeric
                          schema but string date columns
        """
        blocks = list(stream)
        if not blocks:
            return pl.DataFrame().lazy()

        fieldnames = blocks[0].get("line", [])

        # Build CSV string for Polars to parse
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

        csv_string = '\n'.join(csv_lines)

        # Enable numeric inference but DISABLE date parsing.
        # Dates must stay as strings so the harmonise phase can apply
        # UK day-first parsing consistently with the legacy pipeline.
        return pl.read_csv(
            io.StringIO(csv_string),
            try_parse_dates=False,
        ).lazy()

    @staticmethod
    def from_parsed_stream(stream: Iterator[Dict[str, Any]]) -> pl.LazyFrame:
        """
        Convert an already-parsed stream (after NormalisePhase + ParsePhase)
        into a Polars LazyFrame.

        Use this when sharing the legacy Convert/Normalise/Parse phases
        and handing off to polars from ConcatPhase onward.

        Args:
            stream: Iterator yielding blocks with 'row' dicts

        Returns:
            pl.LazyFrame: Polars LazyFrame with inferred schema
        """
        rows: list[dict[str, str]] = []
        for block in stream:
            row = block.get("row")
            if row:
                rows.append(
                    {k: str(v) if v is not None else "" for k, v in row.items()}
                )

        if not rows:
            return pl.LazyFrame()

        return pl.DataFrame(rows).lazy()
