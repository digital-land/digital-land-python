import csv
import json
import pytest

from digital_land.pipeline.task import (
    TaskPipeline,
    TaskPipelineConfig,
    TaskPipelineStatus,
)


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


def _read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def test_log_tasks_creates_row_per_failure(log_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    status = TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )

    assert status == TaskPipelineStatus.SUCCESS
    rows = _read_csv(output)
    assert len(rows) == 2
    assert set(row["task-source"] for row in rows) == {"log"}


def test_log_task_details_json(log_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
    )

    rows = _read_csv(output)
    details = [json.loads(row["details"]) for row in rows]
    statuses = {detail["status"] for detail in details}
    assert 404 in statuses
    assert 500 in statuses
    assert all(isinstance(d["status"], int) for d in details)


def test_log_tasks_empty_when_all_successful(tmp_path):
    log = tmp_path / "log.csv"
    log.write_text(
        "endpoint,resource,status,exception,entry-date,bytes,elapsed\n"
        "endpoint-aaa,resource-aaa,200,,2024-01-01,1234,0.5\n"
    )
    output = tmp_path / "tasks.csv"
    status = TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log,
    )
    assert status == TaskPipelineStatus.NO_TASKS
    assert _read_csv(output) == []


def test_issue_tasks_groups_by_type_and_field(issue_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    status = TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        issue_path=issue_csv,
    )

    assert status == TaskPipelineStatus.SUCCESS
    rows = _read_csv(output)
    assert len(rows) == 2
    assert set(row["task-source"] for row in rows) == {"issue"}


def test_issue_task_details_json(issue_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        issue_path=issue_csv,
    )

    rows = _read_csv(output)
    details = [json.loads(row["details"]) for row in rows]
    geom_task = next(d for d in details if d["issue_type"] == "invalid-geometry")
    assert geom_task["count"] == 2
    assert geom_task["field"] == "geometry"


def test_both_sources_combined(log_csv, issue_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    status = TaskPipeline().run(
        output_path=output,
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        log_path=log_csv,
        issue_path=issue_csv,
    )

    assert status == TaskPipelineStatus.SUCCESS
    rows = _read_csv(output)
    sources = set(row["task-source"] for row in rows)
    assert "log" in sources
    assert "issue" in sources


def test_run_with_config(log_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    config = TaskPipelineConfig(
        dataset="tree-preservation-zone",
        organisation="local-authority-eng:ABC",
        endpoint="endpoint-aaa",
        output_path=output,
        log_path=log_csv,
    )
    status = TaskPipeline(config).run()
    assert status == TaskPipelineStatus.SUCCESS
    assert len(_read_csv(output)) == 2


def test_output_has_correct_columns(log_csv, tmp_path):
    output = tmp_path / "tasks.csv"
    TaskPipeline().run(
        output_path=output,
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
        "task-id",
    }
    rows = _read_csv(output)
    assert set(rows[0].keys()) == expected_cols
