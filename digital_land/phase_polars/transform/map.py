import re
import polars as pl


normalise_pattern = re.compile(r"[^a-z0-9-_]")


def normalise(name):
    new_name = name.replace("_", "-")
    return re.sub(normalise_pattern, "", new_name.lower())


class MapPhase:
    """Rename field names using column map with Polars LazyFrame."""

    def __init__(self, fieldnames, columns=None):
        self.columns = columns or {}
        self.normalised_fieldnames = {normalise(f): f for f in fieldnames}

    def headers(self, fieldnames):
        headers = {}
        matched = []

        for header in sorted(fieldnames):
            fieldname = normalise(header)
            for pattern, value in self.columns.items():
                if fieldname == pattern:
                    matched.append(value)
                    headers[header] = value

        for header in sorted(fieldnames):
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

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply column mapping to LazyFrame.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: LazyFrame with renamed columns
        """
        existing_columns = lf.collect_schema().names()
        headers = self.headers(existing_columns)
        
        rename_map = {}
        columns_to_drop = []
        
        for old_name, new_name in headers.items():
            if new_name == "IGNORE":
                columns_to_drop.append(old_name)
            else:
                rename_map[old_name] = new_name
        
        if columns_to_drop:
            lf = lf.drop(columns_to_drop)
        
        if rename_map:
            lf = lf.rename(rename_map)
        
        return lf
