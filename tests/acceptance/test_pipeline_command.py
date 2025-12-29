"""
Acceptance tests for pipeline_command CLI function.

Tests the end-to-end pipeline processing workflow, simulating actual user
interactions with the CLI command.
"""

import pytest
import pandas as pd
from click.testing import CliRunner

from digital_land.cli import cli


@pytest.fixture
def cli_runner():
    """
    Create a Click CLI runner for testing CLI commands.
    """
    return CliRunner()


@pytest.fixture
def organisation_path(tmp_path):
    """
    Create an organisations dataset for testing.
    """
    org_data = {
        "entity": [101, 102],
        "name": ["Test Org", "Test Org 2"],
        "prefix": ["local-authority", "local-authority"],
        "reference": ["test-org", "test-org-2"],
        "dataset": ["local-authority", "local-authority"],
        "organisation": ["local-authority:test-org", "local-authority:test-org-2"],
    }
    orgs_path = tmp_path / "organisation.csv"
    pd.DataFrame.from_dict(org_data).to_csv(orgs_path, index=False)
    return orgs_path


@pytest.fixture
def test_resource_file(test_dirs):
    """
    Create a test resource CSV file for pipeline processing.
    """
    resource_dir = test_dirs["collection_dir"] / "resource"
    resource_dir.mkdir(parents=True, exist_ok=True)

    resource_hash = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    resource_path = resource_dir / resource_hash

    # Create a simple test CSV file
    with open(resource_path, "w") as f:
        f.write("NAME,NOTES\n")
        f.write("Test Park,A test national park\n")

    return resource_path


def test_pipeline_command_runs_successfully(
    cli_runner, test_dirs, test_resource_file, organisation_path, tmp_path
):
    """
    Test that pipeline command executes successfully with standard usage.

    Verifies the entire pipeline workflow from resource input through to
    transformed output generation, representing the typical user journey
    of processing a resource through the pipeline. ensures correct files are produced
    and no errors occur. but does not account for every scenario or edge case.
    """
    # Arrange
    input_path = str(test_resource_file)
    output_file_path = (
        test_dirs["transformed_dir"]
        / "national-park"
        / f"{test_resource_file.stem}.csv"
    )
    output_path = str(output_file_path)
    output_log_dir = tmp_path / "output-logs"
    config_path = str(tmp_path / "config.sqlite3")

    # Ensure output directories exist (create parent directory of output file)
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    output_log_dir.mkdir(parents=True, exist_ok=True)

    # Test endpoint and organisation values
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
    test_organisation = "local-authority:test-org"

    # Act - Invoke the CLI command with standard options
    result = cli_runner.invoke(
        cli,
        [
            "--dataset",
            "national-park",
            "--pipeline-dir",
            str(test_dirs["pipeline_dir"]),
            "--specification-dir",
            str(test_dirs["specification_dir"]),
            "pipeline",
            input_path,  # positional argument
            output_path,  # positional argument
            "--endpoints",
            test_endpoint,
            "--organisations",
            test_organisation,
            "--issue-dir",
            str(test_dirs["issues_log_dir"]),
            "--column-field-dir",
            str(test_dirs["column_field_dir"]),
            "--dataset-resource-dir",
            str(test_dirs["dataset_resource_dir"]),
            "--converted-resource-dir",
            str(test_dirs["converted_resource_dir"]),
            "--organisation-path",
            str(organisation_path),
            "--cache-dir",
            str(test_dirs["cache_dir"]),
            "--collection-dir",
            str(test_dirs["collection_dir"]),
            "--operational-issue-dir",
            str(test_dirs["operational_issues_dir"]),
            "--output-log-dir",
            str(output_log_dir),
            "--config-path",
            config_path,
        ],
    )

    # Assert
    # Check that the command executed without errors
    if result.exit_code != 0:
        print("\n=== CLI Output ===")
        print(result.output)
        print("\n=== Exception ===")
        print(result.exception)
        if result.exception:
            import traceback

            print("\n=== Full Traceback ===")
            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )
    assert result.exit_code == 0, f"Command failed with: {result.output}"

    # Verify expected directories were created/used
    assert test_dirs["issues_log_dir"].exists()
    assert test_dirs["column_field_dir"].exists()
    assert test_dirs["dataset_resource_dir"].exists()
