import json
import pytest

from digital_land.pipeline.task import TaskPipeline, TaskPipelineConfig


@pytest.fixture
def log_csv(tmp_path):
    """A collection log with one success and two failures."""
    path = tmp_path / "log.csv"
    path.write_text(
        "endpoint,resource,status,exception,entry-date,bytes,elapsed\n"
        "endpoint-aaa,resource-aaa,200,,2024-01-01,1234,0.5\n"
        "endpoint-bbb,resource-bbb,404,,2024-01-01,0,0.1\n"
        "endpoint-ccc,,500,Connection refused,2024-01-01,0,0.0\n"
    )
    return path


@pytest.fixture
def issue_csv(tmp_path):
    """An issue log with error/external issues and one warning to be filtered out."""
    path = tmp_path / "issue.csv"
    path.write_text(
        "dataset,resource,line-number,entry-number,field,entity,issue-type,value,message,severity,responsibility\n"
        "tree-preservation-zone,resource-aaa,1,1,geometry,,invalid-geometry,bad-wkt,Invalid geometry,error,external\n"
        "tree-preservation-zone,resource-aaa,2,2,geometry,,invalid-geometry,other-bad-wkt,Invalid geometry,error,external\n"
        "tree-preservation-zone,resource-aaa,3,3,name,,missing-value,,Missing value,error,external\n"
        "tree-preservation-zone,resource-aaa,4,4,notes,,missing-value,,Missing value,warning,internal\n"
    )
    return path


def test_log_tasks_creates_row_per_failure(log_csv):
    pipeline = TaskPipeline()
    results = pipeline.run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )

    # Only the two failed rows should produce tasks, not the 200
    assert len(results) == 2
    assert set(result["task-source"] for result in results) == {"log"}


def test_log_task_details_json(log_csv):
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )

    details = [json.loads(result["details"]) for result in results]
    statuses = {detail["status"] for detail in details}
    assert 404 in statuses
    assert 500 in statuses
    # status should be an int in the JSON, not a string
    assert all(isinstance(d["status"], int) for d in details)


def test_log_tasks_empty_when_all_successful(tmp_path):
    log = tmp_path / "log.csv"
    log.write_text(
        "endpoint,resource,status,exception,entry-date,bytes,elapsed\n"
        "endpoint-aaa,resource-aaa,200,,2024-01-01,1234,0.5\n"
    )
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log,
    )
    assert results == []


def test_issue_tasks_groups_by_type_and_field(issue_csv):
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        issue_path=issue_csv,
    )

    # 2 geometry rows + 1 name row = 2 groups (invalid-geometry/geometry and missing-value/name)
    # the warning/internal row should be filtered out
    assert len(results) == 2
    assert set(result["task-source"] for result in results) == {"issue"}


def test_issue_task_details_json(issue_csv):
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        issue_path=issue_csv,
    )

    details = [json.loads(result["details"]) for result in results]
    geom_task = next(
        detail for detail in details if detail["issue_type"] == "invalid-geometry"
    )
    assert geom_task["count"] == 2
    assert geom_task["field"] == "geometry"


def test_both_sources_combined(log_csv, issue_csv):
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
        issue_path=issue_csv,
    )

    sources = set(result["task-source"] for result in results)

    assert "log" in sources
    assert "issue" in sources


def test_run_with_config(log_csv):
    config = TaskPipelineConfig(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )
    results = TaskPipeline(config).run()
    assert len(results) == 2


def test_output_has_correct_columns(log_csv):
    results = TaskPipeline().run(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )
    expected_cols = {
        "dataset",
        "organisation",
        "endpoint",
        "resource",
        "details",
        "severity",
        "responsibility",
        "task-source",
        "entry-date",
    }
    assert set(results[0].keys()) == expected_cols
