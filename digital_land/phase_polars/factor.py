import hashlib

import polars as pl

from .phase import PolarsPhase


def fact_hash(entity, field, value):
    data = entity + ":" + field + ":" + value
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class FactorPhase(PolarsPhase):
    """
    Add a fact hash identifier for each fact row.
    """

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if not all(c in df.columns for c in ["entity", "field", "value"]):
            return df

        df = df.with_columns(
            pl.struct(["entity", "field", "value"])
            .map_elements(
                lambda s: fact_hash(
                    str(s["entity"] or ""),
                    str(s["field"] or ""),
                    str(s["value"] or ""),
                )
                if s["entity"]
                else "",
                return_dtype=pl.Utf8,
            )
            .alias("fact")
        )

        return df
