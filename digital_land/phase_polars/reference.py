import re
import logging

import polars as pl

from .phase import PolarsPhase

logger = logging.getLogger(__name__)

curie_re = re.compile(r"(?P<prefix>[A-Za-z0-9_-]+):(?P<reference>[A-Za-z0-9_-].*)$")


def split_curie(value):
    match = curie_re.match(value)
    if not match:
        return ("", value)
    return (match.group("prefix"), match.group("reference"))


class EntityReferencePhase(PolarsPhase):
    """
    Ensure each entry has prefix and reference fields derived from the reference column.
    """

    def __init__(self, dataset=None, prefix=None, issues=None):
        self.dataset = dataset
        self.prefix = prefix or dataset
        self.issues = issues

    def _process_row(self, row_dict):
        reference_value = row_dict.get("reference", "") or row_dict.get(self.dataset, "") or ""
        ref_prefix, reference = split_curie(reference_value)

        if self.issues and ref_prefix:
            self.issues.resource = row_dict.get("__resource", "")
            self.issues.line_number = row_dict.get("__line_number", 0)
            self.issues.entry_number = row_dict.get("__entry_number", 0)
            self.issues.log_issue(
                "reference",
                "reference value contains reference_prefix",
                ref_prefix,
                f"Original reference split into prefix '{ref_prefix}' and reference '{reference}'",
            )

        if "UPRN" in ref_prefix:
            ref_prefix = ""

        prefix = row_dict.get("prefix", "") or ref_prefix or self.prefix
        return prefix, reference

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "prefix" not in df.columns:
            df = df.with_columns(pl.lit("").alias("prefix"))
        if "reference" not in df.columns:
            df = df.with_columns(pl.lit("").alias("reference"))

        prefixes = []
        references = []
        for row in df.iter_rows(named=True):
            p, r = self._process_row(row)
            prefixes.append(p)
            references.append(r)

        df = df.with_columns(
            pl.Series("prefix", prefixes),
            pl.Series("reference", references),
        )

        return df


class FactReferencePhase(PolarsPhase):
    """
    Ensure a fact which is a reference has prefix and reference fields.
    """

    def __init__(
        self,
        field_typology_map=None,
        field_prefix_map=None,
    ):
        self.field_typology_map = field_typology_map or {}
        self.field_prefix_map = field_prefix_map or {}

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "prefix" not in df.columns:
            df = df.with_columns(pl.lit("").alias("prefix"))
        if "reference" not in df.columns:
            df = df.with_columns(pl.lit("").alias("reference"))
        if "field" not in df.columns or "value" not in df.columns:
            return df

        ref_typologies = {
            "category", "document", "geography",
            "organisation", "policy", "legal-instrument",
        }

        def _process(row_dict):
            prefix = row_dict.get("prefix", "") or ""
            reference = row_dict.get("reference", "") or ""

            if prefix and reference:
                return prefix, reference

            field = row_dict.get("field", "")
            typology = self.field_typology_map.get(field, "")

            if typology in ref_typologies:
                value_prefix, value_reference = split_curie(row_dict.get("value", "") or "")
                prefix = prefix or value_prefix or self.field_prefix_map.get(field, field)
                reference = reference or value_reference

            return prefix, reference

        prefixes = []
        references = []
        for row in df.iter_rows(named=True):
            p, r = _process(row)
            prefixes.append(p)
            references.append(r)

        df = df.with_columns(
            pl.Series("prefix", prefixes),
            pl.Series("reference", references),
        )

        return df
