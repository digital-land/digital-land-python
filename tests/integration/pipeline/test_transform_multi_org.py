"""Orchestration tests for multi-organisation resources in Pipeline.transform.

A resource collected from endpoints belonging to more than one organisation
(e.g. a joint local plan two authorities submit to the same URL, producing one
content-hashed resource carrying both orgs) must be processed once PER
organisation — each org set as the default in turn — with the per-org outputs
concatenated into the single resource-keyed file.

These tests exercise that orchestration in isolation: the real phase chain
(which needs a full Specification/Config) is replaced with a fake that records
its call arguments and writes a per-pass CSV, so we can assert HOW transform
drives it without running a real transform. End-to-end reference->entity
resolution for the single-org path is already covered by the acceptance tests.
"""

import types
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from digital_land.pipeline.main import (
    Pipeline,
    _concat_transformed,
    _dedup_log_rows,
)


ORG_A = "local-authority:SOX"
ORG_B = "local-authority:VAL"
RESOURCE = "res-hash-123"


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def test_concat_transformed_merges_rows_under_one_header_and_deletes_inputs(tmp_path):
    a = tmp_path / f"{RESOURCE}.0.csv"
    b = tmp_path / f"{RESOURCE}.1.csv"
    a.write_text("entity,organisation\n100,SOX\n")
    b.write_text("entity,organisation\n100,VAL\n")
    out = tmp_path / f"{RESOURCE}.csv"

    _concat_transformed([a, b], out)

    assert out.read_text() == "entity,organisation\n100,SOX\n100,VAL\n"
    # per-organisation inputs are cleaned up
    assert not a.exists()
    assert not b.exists()


def test_dedup_log_rows_collapses_duplicates_preserving_order():
    log = types.SimpleNamespace(
        rows=[
            {"resource": "r", "column": "ref", "field": "reference"},
            {"resource": "r", "column": "ref", "field": "reference"},  # dup
            {"resource": "r", "column": "geom", "field": "geometry"},
        ]
    )

    _dedup_log_rows(log)

    assert log.rows == [
        {"resource": "r", "column": "ref", "field": "reference"},
        {"resource": "r", "column": "geom", "field": "geometry"},
    ]


# --------------------------------------------------------------------------- #
# transform orchestration
# --------------------------------------------------------------------------- #
@pytest.fixture
def pipeline(tmp_path):
    """A Pipeline against an empty config dir with a stubbed specification.

    The empty pipeline dir means no config files load; the phase chain is
    stubbed per-test so the (Mock) specification is never actually queried.
    """
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    spec = Mock()
    spec.path = str(pipeline_dir)  # so init_logs' hash_directory has a real dir
    return Pipeline(str(pipeline_dir), "test-dataset", specification=spec, config=None)


def _run_transform(pipeline, tmp_path, organisations, **overrides):
    """Drive transform with a fake phase-builder, returning the recorded calls.

    The fake `_build_transform_phases` records its kwargs and writes a per-pass
    CSV (header + one row tagged with that pass's default organisation) to the
    output path it is handed; `run` is a no-op since the file is already written.
    """
    calls = []

    def fake_build(**kwargs):
        calls.append(kwargs)
        out = Path(kwargs["output_path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        org = kwargs["default_values"].get("organisation", "none")
        out.write_text(f"entity,organisation\n100,{org}\n")
        return ["fake-phase"]

    pipeline._build_transform_phases = fake_build
    pipeline.run = Mock()

    output_path = tmp_path / "transformed" / "test-dataset" / f"{RESOURCE}.csv"

    kwargs = dict(
        input_path=str(tmp_path / "input" / RESOURCE),
        output_path=output_path,
        organisation=Mock(),
        resource=RESOURCE,
        valid_category_values={},
        endpoints=["endpoint-1"],
        organisations=organisations,
        converted_path=str(tmp_path / "converted" / f"{RESOURCE}.csv"),
        harmonised_output_path=str(tmp_path / "harmonised" / f"{RESOURCE}.csv"),
        save_harmonised=True,
    )
    kwargs.update(overrides)

    # duplicate_reference_check / count_distinct_entities need real transformed
    # data; patch them out so we isolate the loop-and-concat orchestration.
    with patch(
        "digital_land.pipeline.main.duplicate_reference_check",
        side_effect=lambda issues, csv_path: issues,
    ), patch(
        "digital_land.pipeline.main.count_distinct_entities",
        return_value=1,
    ):
        pipeline.transform(**kwargs)

    return calls, output_path


def test_single_org_runs_once_straight_to_output(pipeline, tmp_path):
    calls, output_path = _run_transform(pipeline, tmp_path, organisations=[ORG_A])

    assert len(calls) == 1
    # writes directly to the real output path — no per-pass temp file
    assert Path(calls[0]["output_path"]) == output_path
    assert calls[0]["default_values"]["organisation"] == ORG_A
    assert calls[0]["providers"] == [ORG_A]
    # side artefacts are produced on this (only) pass
    assert calls[0]["converted_path"] is not None
    assert calls[0]["harmonised_output_path"] is not None
    assert calls[0]["save_harmonised"] is True

    assert output_path.read_text() == f"entity,organisation\n100,{ORG_A}\n"


def test_zero_org_runs_once_with_no_default_organisation(pipeline, tmp_path):
    calls, output_path = _run_transform(pipeline, tmp_path, organisations=[])

    assert len(calls) == 1
    assert "organisation" not in calls[0]["default_values"]
    assert calls[0]["providers"] == []
    assert Path(calls[0]["output_path"]) == output_path


def test_multi_org_runs_once_per_org_and_concatenates(pipeline, tmp_path):
    calls, output_path = _run_transform(
        pipeline, tmp_path, organisations=[ORG_A, ORG_B]
    )

    # one pass per organisation
    assert len(calls) == 2

    # each pass gets its own org as the default and as the sole provider
    assert calls[0]["default_values"]["organisation"] == ORG_A
    assert calls[0]["providers"] == [ORG_A]
    assert calls[1]["default_values"]["organisation"] == ORG_B
    assert calls[1]["providers"] == [ORG_B]

    # each pass writes to its own temp file, not the final output
    assert Path(calls[0]["output_path"]).name == f"{RESOURCE}.0.csv"
    assert Path(calls[1]["output_path"]).name == f"{RESOURCE}.1.csv"
    assert Path(calls[0]["output_path"]) != output_path

    # side artefacts (converted CSV + harmonised) are written on pass 0 only
    assert calls[0]["converted_path"] is not None
    assert calls[0]["harmonised_output_path"] is not None
    assert calls[0]["save_harmonised"] is True
    assert calls[1]["converted_path"] is None
    assert calls[1]["harmonised_output_path"] is None
    assert calls[1]["save_harmonised"] is False

    # both orgs' rows end up in the single resource file, under one header
    assert output_path.read_text() == (
        f"entity,organisation\n100,{ORG_A}\n100,{ORG_B}\n"
    )

    # per-pass temp files are cleaned up by the concat step
    assert not (output_path.parent / f"{RESOURCE}.0.csv").exists()
    assert not (output_path.parent / f"{RESOURCE}.1.csv").exists()

    # the entry-number -> entity map is reset between/after passes so each
    # organisation's issues attach to its own entities
    assert pipeline.issue_log.entry_to_entity == {}
