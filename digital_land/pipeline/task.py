from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import polars as pl
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

logger = logging.getLogger(__name__)

TASK_COLUMNS = [
    "dataset",
    "organisation",
    "endpoint",
    "resource",
    "details",
    "severity",
    "responsibility",
    "task-source",
    "entry-date",
]

_EMPTY_SCHEMA = {col: pl.Utf8 for col in TASK_COLUMNS}


@dataclass(config=ConfigDict(extra="forbid"))
class TaskPipelineConfig:
    dataset: str
    organisation: str
    endpoint: str
    log_path: Optional[Path] = None
    issue_path: Optional[Path] = None
    output_path: Optional[Path] = None
    severity_filter: List[str] = Field(default_factory=lambda: ["error"])
    responsibility_filter: List[str] = Field(default_factory=lambda: ["external"])
    entry_date: Optional[str] = None


class TaskPipeline:
    """Generates a task CSV from collection logs and/or issue logs."""

    def __init__(self, config: Optional[TaskPipelineConfig] = None):
        self.config = config

    def run(
        self,
        dataset: Optional[str] = None,
        organisation: Optional[str] = None,
        endpoint: Optional[str] = None,
        log_path: Optional[Path] = None,
        issue_path: Optional[Path] = None,
        output_path: Optional[Path] = None,
        severity_filter: Optional[List[str]] = None,
        responsibility_filter: Optional[List[str]] = None,
        entry_date: Optional[str] = None,
    ) -> List[dict]:
        cfg = self.config
        dataset = dataset or (cfg.dataset if cfg else None)
        organisation = organisation or (cfg.organisation if cfg else None)
        endpoint = endpoint or (cfg.endpoint if cfg else None)
        log_path = log_path or (cfg.log_path if cfg else None)
        issue_path = issue_path or (cfg.issue_path if cfg else None)
        output_path = output_path or (cfg.output_path if cfg else None)
        severity_filter = severity_filter or (cfg.severity_filter if cfg else ["error"])
        responsibility_filter = responsibility_filter or (
            cfg.responsibility_filter if cfg else ["external"]
        )
        entry_date = entry_date or (cfg.entry_date if cfg else str(date.today()))

        frames = []

        if log_path and Path(log_path).exists():
            log_tasks = _tasks_from_log(log_path, dataset, organisation, entry_date)
            if not log_tasks.is_empty():
                frames.append(log_tasks)

        if issue_path and Path(issue_path).exists():
            issue_tasks = _tasks_from_issues(
                issue_path,
                organisation,
                endpoint,
                severity_filter,
                responsibility_filter,
                entry_date,
            )
            if not issue_tasks.is_empty():
                frames.append(issue_tasks)

        result = pl.concat(frames) if frames else pl.DataFrame(schema=_EMPTY_SCHEMA)
        tasks = result.to_dicts()
        if output_path:
            with open(output_path, "w") as f:
                json.dump(tasks, f, indent=2)
        return tasks


def _tasks_from_log(
    log_path: Path,
    dataset: str,
    organisation: str,
    entry_date: str,
) -> pl.DataFrame:
    """Return a task row for each failed collection log entry."""

    # infer_schema_length=0 tells polars to read all columns as strings rather than
    # infer datatypes based on the first 100 rows, the default behaviour.
    df = pl.read_csv(log_path, infer_schema_length=0, null_values=[""])

    failed = df.filter(pl.col("status") != "200")

    if failed.is_empty():
        return pl.DataFrame(schema=_EMPTY_SCHEMA)

    n = len(failed)

    details_col = failed.select(
        # pl.struct bundles multiple columns so map_elements can access both at once
        pl.struct(["status", "exception"])
        .map_elements(
            lambda row: json.dumps(
                {
                    # status codes are converted to ints and nulls come in as empty strings
                    "status": (
                        int(row["status"])
                        if row["status"] and row["status"].isdigit()
                        else row["status"]
                    ),
                    "exception": row["exception"] or "",
                }
            ),
            return_dtype=pl.Utf8,
        )
        .alias("details")
    )

    return pl.DataFrame(
        {
            "dataset": pl.Series([dataset] * n, dtype=pl.Utf8),
            "organisation": pl.Series([organisation] * n, dtype=pl.Utf8),
            "endpoint": failed["endpoint"],
            "resource": failed["resource"],
            "details": details_col["details"],
            "severity": pl.Series(["error"] * n, dtype=pl.Utf8),
            "responsibility": pl.Series(["external"] * n, dtype=pl.Utf8),
            "task-source": pl.Series(["log"] * n, dtype=pl.Utf8),
            "entry-date": pl.Series([entry_date] * n, dtype=pl.Utf8),
        }
    )


def _tasks_from_issues(
    issue_path: Path,
    organisation: str,
    endpoint: str,
    severity_filter: List[str],
    responsibility_filter: List[str],
    entry_date: str,
) -> pl.DataFrame:
    """Return one task row per (issue-type, resource, field, dataset) group."""

    df = pl.read_csv(issue_path, infer_schema_length=0, null_values=[""])
    cols = set(df.columns)

    if "severity" in cols:
        df = df.filter(pl.col("severity").is_in(severity_filter))
    if "responsibility" in cols:
        df = df.filter(pl.col("responsibility").is_in(responsibility_filter))

    if df.is_empty():
        return pl.DataFrame(schema=_EMPTY_SCHEMA)

    for col in [
        "issue-type",
        "resource",
        "field",
        "dataset",
        "severity",
        "responsibility",
    ]:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))

    # We are counting every row, so there might be duplicate entities across multiple data sources
    # I can add in a split so it behaves more like the current version in submit repo but holding off
    # until I understand the data better and if that split is required.
    grouped = df.group_by(["issue-type", "resource", "field", "dataset"]).agg(
        [
            pl.len().alias("count"),
            pl.first("severity"),
            pl.first("responsibility"),
        ]
    )

    grouped = grouped.with_columns(
        # pl.struct bundles multiple columns so map_elements can access both at once
        pl.struct(["issue-type", "count", "field"])
        .map_elements(
            lambda row: json.dumps(
                {
                    "issue_type": row["issue-type"] or "",
                    "count": row["count"],
                    "field": row["field"] or "",
                }
            ),
            return_dtype=pl.Utf8,
        )
        .alias("details")
    )

    return grouped.select(
        [
            pl.col("dataset"),
            pl.lit(organisation).alias("organisation"),
            pl.lit(endpoint).alias("endpoint"),
            pl.col("resource"),
            pl.col("details"),
            pl.col("severity"),
            pl.col("responsibility"),
            pl.lit("issue").alias("task-source"),
            pl.lit(entry_date).alias("entry-date"),
        ]
    )
