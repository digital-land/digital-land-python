import pytest
from digital_land.log import IssueLog

test_collection_dir = "tests/data"


@pytest.fixture
def sample_issue_log_csv_path():
    return test_collection_dir + "/specification/issue-type.csv"


def test_add_severity_column(sample_issue_log_csv_path):
    issue = IssueLog()
    issue.log_issue("test", "type1", "value1")

    # Confirm 'severity' field is not added to the fieldnames beforehand
    assert "severity" not in issue.fieldnames
    assert "description" not in issue.fieldnames

    issue.add_severity_column(sample_issue_log_csv_path)

    # Check if the 'severity' field is added to fieldnames
    assert "severity" in issue.fieldnames
    assert "description" in issue.fieldnames
    assert issue.rows[0]["severity"] == "sev1"
    assert issue.rows[0]["description"] == "desc1"