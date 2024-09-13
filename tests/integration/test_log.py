from datetime import datetime
import os
from unittest.mock import patch
from digital_land.log import OperationalIssueLog


def mocked_get_now():
    return datetime(2023, 1, 31, 0, 0, 0).isoformat()


def test_operationalIssueLog_save(tmp_path_factory):
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)
    performance_dir = tmp_path_factory.mktemp("performance_issue")
    operational_issue_dir = tmp_path_factory.mktemp("operational_issue")

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(
            operational_issue_dir=operational_issue_dir, performance_dir=performance_dir
        )

        assert os.path.isfile(
            os.path.join(
                performance_dir,
                operational_issue_dir,
                dataset + "/2023-01-31/" + resource + ".csv",
            )
        )


def test_operationalIssueLog_save_path_given(tmp_path_factory):
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)
    performance_dir = tmp_path_factory.mktemp("performance_issue")
    operational_issue_dir = tmp_path_factory.mktemp("operational_issue")
    tmp_dir = tmp_path_factory.mktemp("logdir")
    path = os.path.join(tmp_dir, "opissuelog.csv")

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(
            operational_issue_dir=operational_issue_dir,
            performance_dir=performance_dir,
            path=path,
        )

        assert os.path.isfile(path)
