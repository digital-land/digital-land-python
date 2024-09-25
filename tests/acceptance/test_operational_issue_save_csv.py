import pandas as pd
from digital_land.cli import cli
from click.testing import CliRunner


def test_cli_operational_issue_save_cmd(specification_dir):
    operational_issue_dir = "tests/data/listed-building/performance/operational_issue/"
    dataset = "listed-building-outline"

    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--dataset",
            dataset,
            "operational-issue-save-csv",
            "--operational-issue-dir",
            operational_issue_dir,
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

    assert result.exit_code == 0, f"{result.stdout}"

    df = pd.read_csv(
        "tests/data/listed-building/performance/operational_issue/listed-building-outline/operational-issue.csv"
    )

    assert len(df) == 2
    assert dataset in df["dataset"].values
    assert "listed-building" not in df["dataset"].values
