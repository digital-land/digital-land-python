import pytest
from digital_land.log import IssueLog
from unittest.mock import patch
import pandas as pd


@pytest.fixture
def issue_log_data():
    return [
        {
            "issue-type": "type1",
            "severity": "sev1",
            "name": "test",
            "description": "desc1",
            "responsibility": "internal",
        },
        {
            "issue-type": "type2",
            "severity": "sev2",
            "name": "test",
            "description": "desc2",
            "responsibility": "internal",
        },
    ]


@pytest.fixture
def mapping_data():
    return """
    mappings:
    - field: test
      issue-type: type2
      description: appended description
    """


def test_add_severity_column(issue_log_data):
    issue = IssueLog()
    issue.log_issue("test", "type1", "value1")

    # Confirm 'severity' field is not added to the fieldnames beforehand
    assert "severity" not in issue.fieldnames
    # assert "description" not in issue.fieldnames

    with patch("pandas.read_csv", return_value=pd.DataFrame(issue_log_data)):
        # Call the add_severity_column method with the fake severity_mapping
        issue.add_severity_column("fake_file_path.csv")

    # Check if the 'severity' field is added to fieldnames
    assert "severity" in issue.fieldnames
    # assert "description" in issue.fieldnames
    assert issue.rows[0]["severity"] == "sev1"
    # assert issue.rows[0]["description"] == "desc1"


# def test_appendErrorMessage(issue_log_data, mapping_data):
#     issue = IssueLog()
#     issue.log_issue("test", "type1", "value1")
#     issue.log_issue("test", "type2", "value2")

#     with patch("pandas.read_csv", return_value=pd.DataFrame(issue_log_data)):
#         # Call the add_severity_column method with the fake severity_mapping
#         issue.add_severity_column("fake_file_path.csv")

#     assert issue.rows[0]["description"] == "desc1"
#     assert issue.rows[1]["description"] == "desc2"

#     # Patch the open function to return the fake YAML content
#     with patch("builtins.open", mock_open(read_data=mapping_data)):
#         # Call the appendErrorMessage method with the fake mapping YAML
#         issue.appendErrorMessage("fake_yaml_path.yaml")

#     assert issue.rows[0]["description"] == "desc1"
#     assert issue.rows[1]["description"] == "appended description"
