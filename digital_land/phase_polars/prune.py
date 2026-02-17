import logging

import polars as pl

from .phase import PolarsPhase

logger = logging.getLogger(__name__)


class FieldPrunePhase(PolarsPhase):
    """
    Reduce columns to only those specified for the dataset.
    """

    def __init__(self, fields):
        self.fields = list(
            set(fields + ["entity", "organisation", "prefix", "reference"])
        )
        logging.debug(f"pruning fields to {self.fields}")

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        meta_cols = [c for c in df.columns if c.startswith("__")]
        keep = [c for c in self.fields if c in df.columns] + meta_cols
        return df.select(keep)


class EntityPrunePhase(PolarsPhase):
    """
    Remove entries with a missing entity value.
    """

    def __init__(self, issue_log=None, dataset_resource_log=None):
        self.log = dataset_resource_log

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            if self.log:
                self.log.entry_count = 0
            return df

        if "entity" not in df.columns:
            if self.log:
                self.log.entry_count = 0
            return df

        # Log skipped rows
        missing = df.filter(
            pl.col("entity").is_null() | (pl.col("entity") == "")
        )
        for row in missing.iter_rows(named=True):
            resource = row.get("__resource", "")
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            curie = f"{prefix}:{reference}"
            entry_number = row.get("__entry_number", "")
            logger.info(f"{resource} row {entry_number}: missing entity for {curie}")

        result = df.filter(
            pl.col("entity").is_not_null() & (pl.col("entity") != "")
        )

        if self.log:
            self.log.entry_count = result.height

        return result


class FactPrunePhase(PolarsPhase):
    """
    Remove facts with a missing value (except when field is end-date).
    """

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "value" not in df.columns:
            return df

        return df.filter(
            (pl.col("value").is_not_null() & (pl.col("value") != ""))
            | (pl.col("field") == "end-date")
        )
