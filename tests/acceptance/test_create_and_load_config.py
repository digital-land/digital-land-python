"""
Module to test the create and loading on config sqlite from the pipeline directory, uses the current specification.
"""

import pytest
import pandas as pd
from click.testing import CliRunner

from digital_land.cli import cli


@pytest.fixture(scope="module")
def pipeline_dir(tmp_path_factory):
    """
    We create a pipeline direcctory  to be used for loading in
    """
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    test_data = [
        {
            "dataset": "conservation-area",
            "entity-minimum": 44000001,
            "entity-maximum": 44000001,
            "organisation": "local-authority:SAL",
        }
    ]
    test_df = pd.DataFrame(test_data)
    test_df.to_csv(pipeline_dir / "entity-organisation.csv")

    expect_test_data = [
        {
            "datasets": "test",
            "organisations": "local-authority:test",
            "name": "test rule for {{ organisation.name }}",
            "operation": "count_lpa_boundary",
            "parameters": '{"expected":1,"lpa":"{{ organisation.local_planning_authority }}","organisation_entity":"{{ organisation.entity }}"}',
        }
    ]

    expect_test_data_df = pd.DataFrame(expect_test_data)
    expect_test_data_df.to_csv(pipeline_dir / "expect.csv")

    return pipeline_dir


def test_create_and_load(specification_dir, pipeline_dir, tmp_path):
    config_path = tmp_path / "config.sqlite3"

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--specification-dir",
            str(specification_dir),
            "--pipeline-dir",
            str(pipeline_dir),
            "config-create",
            "--config-path",
            str(config_path),
        ],
    )

    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0, "config file not created successfully"

    result = runner.invoke(
        cli,
        [
            "--specification-dir",
            str(specification_dir),
            "--pipeline-dir",
            str(pipeline_dir),
            "config-load",
            "--config-path",
            str(config_path),
        ],
    )

    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0
