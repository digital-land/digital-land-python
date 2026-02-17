import polars as pl

from .phase import PolarsPhase


class PivotPhase(PolarsPhase):
    """
    Unpivot entity rows into a series of facts (one row per field value).
    """

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        meta_cols = [c for c in df.columns if c.startswith("__")]
        data_cols = [c for c in df.columns if not c.startswith("__") and c != "entity"]

        if "entity" not in df.columns:
            return df

        # We need to carry metadata and entity through the unpivot.
        # Polars .unpivot() works on value columns.
        # Build the result row-by-row for exact parity with the streaming version.
        rows = []
        for row in df.iter_rows(named=True):
            entity = row.get("entity", "")
            resource = row.get("__resource", "")
            line_number = row.get("__line_number", 0)
            entry_number = row.get("__entry_number", 0)
            priority = row.get("__priority", 1)
            entry_date = row.get("entry-date", "")

            for field in sorted(data_cols):
                value = row.get(field, "") or ""
                rows.append(
                    {
                        "fact": "",
                        "entity": entity,
                        "field": field,
                        "value": value,
                        "priority": str(priority),
                        "resource": resource,
                        "line-number": str(line_number),
                        "entry-number": str(entry_number),
                        "entry-date": entry_date,
                        "__resource": resource,
                        "__line_number": line_number,
                        "__entry_number": entry_number,
                    }
                )

        if not rows:
            return pl.DataFrame(
                schema={
                    "fact": pl.Utf8,
                    "entity": pl.Utf8,
                    "field": pl.Utf8,
                    "value": pl.Utf8,
                    "priority": pl.Utf8,
                    "resource": pl.Utf8,
                    "line-number": pl.Utf8,
                    "entry-number": pl.Utf8,
                    "entry-date": pl.Utf8,
                    "__resource": pl.Utf8,
                    "__line_number": pl.Int64,
                    "__entry_number": pl.Int64,
                }
            )

        return pl.DataFrame(rows)
