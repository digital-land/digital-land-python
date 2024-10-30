from datetime import datetime
import os
from unittest.mock import patch
from digital_land.log import Log, OperationalIssueLog


def mocked_get_now():
    return datetime(2023, 1, 31, 0, 0, 0).isoformat()


def test_operationalIssueLog_save(tmp_path_factory):
    dataset = "dataset"
    resource = "resource"
    operational_issue = OperationalIssueLog(dataset=dataset, resource=resource)
    performance_dir = tmp_path_factory.mktemp("performance")
    operational_issue_dir = "operational_issue"

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(
            output_dir=os.path.join(performance_dir, operational_issue_dir)
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
    performance_dir = tmp_path_factory.mktemp("performance")
    operational_issue_dir = "operational_issue"
    tmp_dir = tmp_path_factory.mktemp("logdir")
    path = os.path.join(tmp_dir, "opissuelog.csv")

    with patch(
        "digital_land.log.OperationalIssueLog.get_now", side_effect=mocked_get_now
    ):
        operational_issue.save(
            output_dir=os.path.join(operational_issue_dir, performance_dir),
            path=path,
        )

        assert os.path.isfile(path)


def test_operationalIssueLog_load_log_items():
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue = OperationalIssueLog(
        dataset=dataset,
        resource=resource,
        operational_issue_dir="tests/data/listed-building/performance/operational_issue/",
    )

    operational_issue.load_log_items()
    assert len(operational_issue.operational_issues.entries) == 2
    assert (
        operational_issue.operational_issues.entries[0]["issue-type"]
        == "unknown entity"
    )
    assert (
        operational_issue.operational_issues.entries[0]["value"]
        == "listed-building-outline:2"
    )
    assert operational_issue.operational_issues.entries[0]["dataset"] == dataset


def test_operationalIssueLog_load_log_items_after():
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue = OperationalIssueLog(
        dataset=dataset,
        resource=resource,
        operational_issue_dir="tests/data/listed-building/performance/operational_issue/",
    )

    operational_issue.load_log_items(after="2030-09-20")
    assert len(operational_issue.operational_issues.entries) == 0


def test_operationalIssueLog_load():
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue = OperationalIssueLog(
        dataset=dataset,
        resource=resource,
        operational_issue_dir="tests/data/listed-building/performance/operational_issue/",
    )

    operational_issue.load()
    assert len(operational_issue.operational_issues.entries) == 2
    assert (
        operational_issue.operational_issues.entries[0]["issue-type"]
        == "unknown entity"
    )
    assert (
        operational_issue.operational_issues.entries[0]["value"]
        == "listed-building-outline:2"
    )
    assert operational_issue.operational_issues.entries[0]["dataset"] == dataset


def test_operationalIssueLog_load_with_csv():
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue_dir = (
        "tests/data/listed-building/performance_csv/operational_issue_csv/"
    )
    operational_issue = OperationalIssueLog(
        dataset=dataset, resource=resource, operational_issue_dir=operational_issue_dir
    )

    operational_issue.load(operational_issue_directory=operational_issue_dir)
    assert len(operational_issue.operational_issues.entries) == 2
    assert (
        operational_issue.operational_issues.entries[0]["issue-type"]
        == "unknown entity"
    )
    assert (
        operational_issue.operational_issues.entries[0]["value"]
        == "listed-building-outline:2"
    )
    assert operational_issue.operational_issues.entries[0]["dataset"] == dataset


def test_operationalIssueLog_update():
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue_dir = "tests/data/listed-building/performance/operational_issue/"

    operational_issue = OperationalIssueLog(
        dataset=dataset, resource=resource, operational_issue_dir=operational_issue_dir
    )

    operational_issue.operational_issues._latest_entry_date = "2024-09-18"
    operational_issue.update()
    assert len(operational_issue.operational_issues.entries) == 1


def test_operationalIssueLog_save_csv(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "resource"
    operational_issue_dir = "tests/data/listed-building/performance/operational_issue/"
    tmp_dir = tmp_path_factory.mktemp("temp_dir")

    operational_issue = OperationalIssueLog(
        dataset=dataset, resource=resource, operational_issue_dir=operational_issue_dir
    )

    operational_issue.save_csv(directory=tmp_dir)

    assert os.path.isfile(os.path.join(tmp_dir, dataset, "operational-issue.csv"))


def test_log_save_parquet(tmp_path_factory):
    dataset = "listed-building-outline"
    resource = "resource"
    log = Log()
    log.dataset = dataset
    log.resource = resource
    log.rows = [
        {"dataset": dataset, "resource": resource, "issue": "issue1"},
        {"dataset": dataset, "resource": resource, "issue": "issue2"},
    ]
    output_dir = tmp_path_factory.mktemp("parquet")
    output_dir = "test"

    log.save_parquet(output_dir)
    parquet_path = os.path.join(
        output_dir, f"dataset={dataset}/resource={resource}/{resource}-0.parquet"
    )
    assert os.path.isfile(parquet_path)


# def test_log_save_parquet_no_rows(tmp_path_factory):
#     dataset = "listed-building-outline"
#     resource = "resource"
#     log = Log()
#     log.dataset = dataset
#     log.resource = resource
#     output_dir = tmp_path_factory.mktemp("parquet")

#     log.save_parquet(output_dir)

#     parquet_path = os.path.join(output_dir,"/dataset=listed-building-outline/resource=resource/resource-0.parquet")
#     assert os.path.isfile(parquet_path)
