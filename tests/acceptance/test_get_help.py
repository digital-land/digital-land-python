"""
Module to test that a user can run digital-land --help to get descriptions of the digital-land-command
"""

from click.testing import CliRunner

from digital_land.cli import cli


def test_help_on_context():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0
