import itertools

import polars as pl

from .phase import PolarsPhase


class ConcatFieldPhase(PolarsPhase):
    """
    Concatenate multiple source fields into a single destination field.
    """

    def __init__(self, concats=None, log=None):
        if concats is None:
            concats = {}
        self.concats = concats

        if log:
            for fieldname, cat in self.concats.items():
                log.add(
                    fieldname,
                    cat["prepend"]
                    + cat["separator"].join(cat["fields"])
                    + cat["append"],
                )

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0 or not self.concats:
            return df

        for fieldname, cat in self.concats.items():
            prepend = cat["prepend"]
            separator = cat["separator"]
            append = cat["append"]
            source_fields = cat["fields"]

            # Ensure the destination column exists
            if fieldname not in df.columns:
                df = df.with_columns(pl.lit("").alias(fieldname))

            # Build list of expressions for values to concatenate
            # Start with the existing field value, then add source fields
            parts = [pl.col(fieldname).fill_null("")]
            for h in source_fields:
                if h in df.columns:
                    parts.append(
                        pl.when(
                            pl.col(h).is_not_null()
                            & (pl.col(h).str.strip_chars() != "")
                        )
                        .then(pl.col(h))
                        .otherwise(pl.lit(None))
                    )

            # Filter out nulls and join with separator, then wrap with prepend/append
            def _concat_row(row_vals):
                filtered = [v for v in row_vals if v is not None and v != ""]
                body = separator.join(filtered)
                return prepend + body + append

            # Use struct + map_elements for the concatenation logic
            struct_cols = []
            temp_names = []
            for i, part in enumerate(parts):
                name = f"__concat_part_{i}"
                temp_names.append(name)
                struct_cols.append(part.alias(name))

            df = df.with_columns(struct_cols)

            df = df.with_columns(
                pl.struct(temp_names)
                .map_elements(
                    lambda s, sep=separator, pre=prepend, app=append: (
                        pre
                        + sep.join(
                            v
                            for v in s.values()
                            if v is not None and str(v).strip() != ""
                        )
                        + app
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias(fieldname)
            )

            df = df.drop(temp_names)

        return df
