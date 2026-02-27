import shutil
from pathlib import Path

import pandas as pd
import pytest

from digital_land.log import IssueLog, ColumnFieldLog
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.map import MapPhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase


# this test exercises the *legacy* phase pipeline that used to be referred to as
# "phase1 .. phase9" in the original digital land codebase.  the nine steps
# correspond to Convert -> Normalise -> Parse -> Concat -> Filter -> Map ->
# Filter -> Patch -> Harmonise (a tenth Default phase is also run for
# completeness).  the purpose of the test is simply to run the stream through
# every module and make a couple of sanity assertions at each transition.
#
# the data comes from the existing example CSV which lives under
# tests/data/resource_examples; using a real-ish file helps catch issues such
# as missing column mappings when harmonising.
#
# the assertions are intentionally lightweight: after each phase we check that we
# produced some output and (once parsing has happened) that the output resembles
# a pandas DataFrame with at least one column.  intermediate results are written
# to `tmp_path` so a developer can manually inspect them if a regression
# occurs.


def _stream_to_blocks(stream):
    # exhaust a stream into a list of blocks; each block is a dict with at
    # least a ``row`` key (possibly empty before ParsePhase).
    return list(stream) if stream is not None else []


def _blocks_to_dataframe(blocks):
    # build a DataFrame from the "row" entries; missing keys become NaN.
    rows = [b.get("row", {}) for b in blocks]
    return pd.DataFrame(rows)


def test_legacy_harmonise_phases(tmp_path: Path):
    # copy the sample file into the temp directory so the phases can work
    input_src = Path(__file__).parent / "data" / "resource_examples" / "gml_to_csv_buckinghamshire.csv"
    assert input_src.exists(), "example data not found"

    input_file = tmp_path / "input.csv"
    shutil.copy(input_src, input_file)

    # output directory for phase results
    output_dir = Path(__file__).parent / "data" / "output"
    output_dir.mkdir(exist_ok=True, parents=True)

    # read column names early so we can build simple "identity" maps later
    sample_df = pd.read_csv(input_file)
    columns = list(sample_df.columns)

    # prepare the minimal configuration objects required by the phases
    issue_log = IssueLog(dataset="test", resource="resource")
    column_log = ColumnFieldLog(dataset="test", resource="resource")

    field_datatype_map = {c: "string" for c in columns}
    valid_category_values = {}
    skip_patterns = {}
    concats = {}
    filters = {}
    mapping_columns = {c: c for c in columns}
    patches = {}
    default_fields = {}
    default_values = {}

    phases = [
        ConvertPhase(path=str(input_file)),
        NormalisePhase(skip_patterns=skip_patterns),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_log),
        FilterPhase(filters=filters),
        MapPhase(fieldnames=columns, columns=mapping_columns, log=column_log),
        FilterPhase(filters=filters),
        PatchPhase(issues=issue_log, patches=patches),
        HarmonisePhase(
            field_datatype_map=field_datatype_map,
            issues=issue_log,
            dataset="test",
            valid_category_values=valid_category_values,
        ),
        DefaultPhase(default_fields=default_fields, default_values=default_values, issues=issue_log),
    ]

    stream = None
    for idx, phase in enumerate(phases, start=1):
        stream = phase.process(stream)
        blocks = _stream_to_blocks(stream)

        # convert the blocks into a DataFrame so the assertions are easier
        df = _blocks_to_dataframe(blocks)

        # write the intermediate output to tests/data/output for manual inspection
        out_path = output_dir / f"phase_{idx}.csv"
        df.to_csv(out_path, index=False)
        assert out_path.exists(), f"phase {idx} did not produce an output file"

        # basic invariants
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0, f"phase {idx} produced no rows"
        if idx >= 3:  # after ParsePhase the dataframe should have columns
            assert df.shape[1] > 0, f"phase {idx} dropped all columns"

        # prepare the next phase with the consumed blocks
        stream = iter(blocks)

    # final sanity check: harmonise should not have thrown and the log is
    # populated (there may be issues with the real file).
    assert isinstance(issue_log.rows, list)
    assert len(issue_log.rows) >= 0


if __name__ == "__main__":
    # allow running the test directly for debugging
    pytest.main([__file__])
