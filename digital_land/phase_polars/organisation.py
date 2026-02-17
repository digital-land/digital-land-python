import polars as pl

from .phase import PolarsPhase


class OrganisationPhase(PolarsPhase):
    """
    Look up the organisation value.
    """

    def __init__(self, organisation=None, issues=None):
        self.organisation = organisation
        self.issues = issues

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "organisation" not in df.columns:
            df = df.with_columns(pl.lit("").alias("organisation"))

        if self.organisation is None:
            return df

        # Apply organisation lookup row-by-row (lookup may be complex)
        def _lookup(val):
            result = self.organisation.lookup(val if val else "")
            return result if result else ""

        df = df.with_columns(
            pl.col("organisation")
            .map_elements(_lookup, return_dtype=pl.Utf8)
            .alias("__org_resolved")
        )

        # Log issues for rows where organisation could not be resolved
        if self.issues:
            for row in df.filter(pl.col("__org_resolved") == "").iter_rows(named=True):
                org_val = row.get("organisation", "")
                if org_val:
                    self.issues.resource = row.get("__resource", "")
                    self.issues.line_number = row.get("__line_number", 0)
                    self.issues.entry_number = row.get("__entry_number", 0)
                    self.issues.log_issue(
                        "organisation", "invalid organisation", org_val
                    )

        df = df.with_columns(
            pl.col("__org_resolved").alias("organisation")
        ).drop("__org_resolved")

        return df
