from copy import deepcopy

import polars as pl

from .phase import PolarsPhase

try:
    from shapely.ops import unary_union
    from shapely.geometry import MultiPolygon
    import shapely.wkt
    from digital_land.datatype.wkt import dump_wkt

    HAS_SHAPELY = True
except ImportError:
    HAS_SHAPELY = False


def combine_geometries(wkts, precision=6):
    geometries = [shapely.wkt.loads(x) for x in wkts]
    union = unary_union(geometries)
    if not isinstance(union, MultiPolygon):
        union = MultiPolygon([union])
    return dump_wkt(union, precision=precision)


class FactCombinePhase(PolarsPhase):
    """
    Combine field values from multiple facts for the same entity.
    """

    def __init__(self, issue_log=None, fields=None):
        if fields is None:
            fields = {}
        self.issues = issue_log
        self.fields = fields

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0 or not self.fields:
            return df

        if "field" not in df.columns or "entity" not in df.columns:
            return df

        combine_field_names = set(self.fields.keys()) if isinstance(self.fields, dict) else set(self.fields)

        # Split into combinable and non-combinable
        mask = pl.col("field").is_in(list(combine_field_names))
        pass_through = df.filter(~mask)
        to_combine = df.filter(mask)

        if to_combine.height == 0:
            return pass_through

        # Group by entity + field and combine values
        combined_rows = []
        for (entity, field), group_df in to_combine.group_by(["entity", "field"]):
            values = [
                v
                for v in group_df["value"].to_list()
                if v is not None and v != ""
            ]
            values = sorted(set(values))

            if field == "geometry" and HAS_SHAPELY and values:
                combined_value = combine_geometries(values)
            elif isinstance(self.fields, dict) and field in self.fields:
                separator = self.fields[field]
                combined_value = separator.join(values)
            else:
                combined_value = ";".join(values)

            # Emit rows for each original row in the group
            for row in group_df.iter_rows(named=True):
                if self.issues:
                    self.issues.line_number = row.get("line-number", row.get("__line_number", ""))
                    self.issues.entry_number = row.get("entry-number", row.get("__entry_number", ""))
                    self.issues.log_issue(field, "combined-value", entity)

                new_row = dict(row)
                new_row["value"] = combined_value
                combined_rows.append(new_row)

        if combined_rows:
            combined_df = pl.DataFrame(combined_rows, schema=df.schema)
            return pl.concat([pass_through, combined_df])

        return pass_through
