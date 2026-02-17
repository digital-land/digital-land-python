import logging

import polars as pl

from .phase import PolarsPhase
from digital_land.configuration.main import Config


class PriorityPhase(PolarsPhase):
    """
    Deduce the priority of each entry when assembling facts.
    """

    def __init__(self, config: Config = None, providers=None):
        if providers is None:
            providers = []
        self.providers = providers
        self.default_priority = 1
        self.config = config
        if not config:
            logging.warning(
                f"No config provided so priority defaults to {self.default_priority}"
            )

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "entity" not in df.columns:
            df = df.with_columns(pl.lit(self.default_priority).alias("__priority"))
            return df

        if self.config:
            priorities = []
            organisations = []
            for row in df.iter_rows(named=True):
                entity = row.get("entity", "")
                authoritative_org = self.config.get_entity_organisation(entity)
                if authoritative_org is not None:
                    if authoritative_org in self.providers:
                        priorities.append(2)
                        organisations.append(row.get("organisation", ""))
                    else:
                        priorities.append(self.default_priority)
                        organisations.append(authoritative_org)
                else:
                    priorities.append(self.default_priority)
                    organisations.append(row.get("organisation", ""))

            df = df.with_columns(
                pl.Series("__priority", priorities),
                pl.Series("organisation", organisations),
            )
        else:
            df = df.with_columns(
                pl.lit(self.default_priority).alias("__priority")
            )

        return df
