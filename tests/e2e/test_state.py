import os
import json
import subprocess
from pathlib import Path
from digital_land.commands import save_state, compare_state

specification_hash = "ebe620f5228d01170b1857bad3e738aa432f5fd6"
collection_hash = "ed4c5979268ad880f7edbdc2047cfcfa6b9ee3b4"
pipeline_hash = "4a5a778d678db812e4f3d498a5aaa6f39af38d10"


def get_code_hash():
    proc = subprocess.run(
        "git log -n 1".split(), cwd=os.path.dirname(__file__), stdout=subprocess.PIPE
    )
    # the first line of this is "commit <hash>"
    hash = proc.stdout.splitlines()[0].split()[1].decode()
    return hash


def test_state(tmp_path):
    state_path = os.path.join(tmp_path, "state.json")
    test_data_dir = Path("tests/data/state")

    save_state(
        specification_dir=os.path.join(test_data_dir, "specification"),
        collection_dir=os.path.join(test_data_dir, "collection"),
        pipeline_dir=os.path.join(test_data_dir, "pipeline"),
        output_path=state_path,
    )

    with open(state_path, "r") as json_file:
        state_data = json.load(json_file)

        assert list(state_data.keys()) == [
            "code",
            "specification",
            "collection",
            "pipeline",
        ]
        assert state_data["code"] == get_code_hash()
        assert state_data["specification"] == specification_hash
        assert state_data["collection"] == collection_hash
        assert state_data["pipeline"] == pipeline_hash

    assert (
        compare_state(
            specification_dir=os.path.join(test_data_dir, "specification"),
            collection_dir=os.path.join(test_data_dir, "collection"),
            pipeline_dir=os.path.join(test_data_dir, "pipeline"),
            state_path=state_path,
        )
        is None
    )

    assert (
        compare_state(
            specification_dir=os.path.join(test_data_dir, "specification"),
            collection_dir=os.path.join(test_data_dir, "collection_exclude"),
            pipeline_dir=os.path.join(test_data_dir, "pipeline"),
            state_path=state_path,
        )
        is None
    )

    assert compare_state(
        specification_dir=os.path.join(test_data_dir, "specification"),
        collection_dir=os.path.join(test_data_dir, "collection_blank"),
        pipeline_dir=os.path.join(test_data_dir, "pipeline"),
        state_path=state_path,
    ) == ["collection"]
