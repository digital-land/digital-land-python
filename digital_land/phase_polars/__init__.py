"""
Polars-based pipeline phases.

Drop-in replacements for the streaming phases in `digital_land.phase`.
Each phase accepts and returns a `polars.DataFrame` instead of a generator.
"""

import logging

import polars as pl

from .phase import PolarsPhase
from .convert import ConvertPhase
from .normalise import NormalisePhase
from .concat import ConcatFieldPhase
from .filter import FilterPhase
from .map import MapPhase
from .patch import PatchPhase
from .harmonise import HarmonisePhase
from .default import DefaultPhase
from .migrate import MigratePhase
from .organisation import OrganisationPhase
from .prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from .reference import EntityReferencePhase, FactReferencePhase
from .prefix import EntityPrefixPhase
from .lookup import EntityLookupPhase, FactLookupPhase, PrintLookupPhase
from .save import SavePhase
from .pivot import PivotPhase
from .combine import FactCombinePhase
from .factor import FactorPhase
from .priority import PriorityPhase
from .dump import DumpPhase
from .load import LoadPhase

logger = logging.getLogger(__name__)


def run_polars_pipeline(*phases):
    """
    Run a sequence of Polars phases.

    Each phase receives the DataFrame output of the previous phase.
    The first phase typically starts from ``df=None`` and creates
    the initial DataFrame (e.g. ConvertPhase).
    """
    df = None
    for phase in phases:
        logger.debug(f"running polars phase {phase.__class__.__name__}")
        df = phase.process(df)
        if df is not None:
            logger.debug(
                f"  -> {phase.__class__.__name__} produced {df.height} rows, "
                f"{len([c for c in df.columns if not c.startswith('__')])} data cols"
            )
    return df


__all__ = [
    "PolarsPhase",
    "ConvertPhase",
    "NormalisePhase",
    "ConcatFieldPhase",
    "FilterPhase",
    "MapPhase",
    "PatchPhase",
    "HarmonisePhase",
    "DefaultPhase",
    "MigratePhase",
    "OrganisationPhase",
    "FieldPrunePhase",
    "EntityPrunePhase",
    "FactPrunePhase",
    "EntityReferencePhase",
    "FactReferencePhase",
    "EntityPrefixPhase",
    "EntityLookupPhase",
    "FactLookupPhase",
    "PrintLookupPhase",
    "SavePhase",
    "PivotPhase",
    "FactCombinePhase",
    "FactorPhase",
    "PriorityPhase",
    "DumpPhase",
    "LoadPhase",
    "run_polars_pipeline",
]
