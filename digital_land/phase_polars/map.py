import re

import polars as pl

from ..log import ColumnFieldLog
from .phase import PolarsPhase

normalise_pattern = re.compile(r"[^a-z0-9-_]")


def normalise(name):
    new_name = name.replace("_", "-")
    return re.sub(normalise_pattern, "", new_name.lower())


class MapPhase(PolarsPhase):
    """
    Rename columns according to the column map and specification fieldnames.
    """

    def __init__(self, fieldnames, columns=None, log=None):
        if columns is None:
            columns = {}
        self.columns = columns
        self.normalised_fieldnames = {normalise(f): f for f in fieldnames}
        if not log:
            log = ColumnFieldLog()
        self.log = log

    def headers(self, column_names):
        """Build the header mapping (column_name → field_name)."""
        headers = {}
        matched = []

        for header in sorted(column_names):
            fieldname = normalise(header)
            for pattern, value in self.columns.items():
                if fieldname == pattern:
                    matched.append(value)
                    headers[header] = value

        for header in sorted(column_names):
            if header in headers:
                continue
            fieldname = normalise(header)
            if fieldname not in matched and fieldname in self.normalised_fieldnames:
                headers[header] = self.normalised_fieldnames[fieldname]

        if {"GeoX", "Easting"} <= headers.keys():
            item = headers.pop("GeoX")
            headers["GeoX"] = item

        if {"GeoY", "Northing"} <= headers.keys():
            item = headers.pop("GeoY")
            headers["GeoY"] = item

        return headers

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        data_cols = [c for c in df.columns if not c.startswith("__")]
        header_map = self.headers(data_cols)

        # Log headers
        for col, field in header_map.items():
            self.log.add(column=col, field=field)

        # Select only mapped columns (drop unmapped data cols), keep metadata
        meta_cols = [c for c in df.columns if c.startswith("__")]

        select_exprs = []
        for col, field in header_map.items():
            if field == "IGNORE":
                continue
            select_exprs.append(pl.col(col).fill_null("").alias(field))

        # Add metadata columns
        for mc in meta_cols:
            select_exprs.append(pl.col(mc))

        # Handle duplicate target field names - if multiple columns map to the same
        # field, keep the last one (matching original generator behaviour)
        seen = {}
        unique_exprs = []
        for expr in select_exprs:
            # Get the output name from the expression
            name = expr.meta.output_name()
            seen[name] = expr
        unique_exprs = list(seen.values())

        return df.select(unique_exprs)
