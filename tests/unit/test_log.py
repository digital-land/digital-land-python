import pytest
from digital_land.log import IssueLog, OperationalIssueLog
from unittest.mock import patch, mock_open
from datetime import datetime
import pandas as pd
import os


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
    assert "description" not in issue.fieldnames
    assert "responsibility" not in issue.fieldnames
    with patch("pandas.read_csv", return_value=pd.DataFrame(issue_log_data)):
        # Call the add_severity_column method with the fake severity_mapping
        issue.add_severity_column("fake_file_path.csv")

    # Check if the 'severity' field is added to fieldnames
    assert "severity" in issue.fieldnames
    assert "description" in issue.fieldnames
    assert "responsibility" in issue.fieldnames
    assert issue.rows[0]["severity"] == "sev1"
    assert issue.rows[0]["description"] == "desc1"
    assert issue.rows[0]["responsibility"] == "internal"


def test_appendErrorMessage(issue_log_data, mapping_data):
    issue = IssueLog()
    issue.log_issue("test", "type1", "value1")
    issue.log_issue("test", "type2", "value2")

    with patch("pandas.read_csv", return_value=pd.DataFrame(issue_log_data)):
        # Call the add_severity_column method with the fake severity_mapping
        issue.add_severity_column("fake_file_path.csv")

    assert issue.rows[0]["description"] == "desc1"
    assert issue.rows[1]["description"] == "desc2"

    # Patch the open function to return the fake YAML content
    with patch("builtins.open", mock_open(read_data=mapping_data)):
        # Call the appendErrorMessage method with the fake mapping YAML
        issue.appendErrorMessage("fake_yaml_path.yaml")

    assert issue.rows[0]["description"] == "desc1"
    assert issue.rows[1]["description"] == "appended description"


def mocked_get_now():
    return datetime(2023, 1, 31, 0, 0, 0).isoformat()


def test_operationalIssueLog_save(tmp_path_factory):
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)
    operational_issue_dir = tmp_path_factory.mktemp("operational_issue")

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(operational_issue_dir=operational_issue_dir)

        assert os.path.isfile(
            os.path.join(
                operational_issue_dir, "2023-01-31/" + dataset + "/" + resource + ".csv"
            )
        )


def test_operationalIssueLog_save_path_given(tmp_path_factory):
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)
    operational_issue_dir = tmp_path_factory.mktemp("operational_issue")
    tmp_dir = tmp_path_factory.mktemp("logdir")
    path = os.path.join(tmp_dir, "opissuelog.csv")

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(operational_issue_dir=operational_issue_dir, path=path)

        assert os.path.isfile(path)


def test_operationalIssueLog_save_no_operational_dir():
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)

    files_before = []
    for root, dirs, files in os.walk(os.getcwd()):
        files_before.extend(files)

    operational_issue.save()

    files_after = []
    for root, dirs, files in os.walk(os.getcwd()):
        files_after.extend(files)

    assert files_before == files_after
