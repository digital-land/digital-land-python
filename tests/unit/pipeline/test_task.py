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


class TestTaskPipeline:

    def test_run_log_tasks_creates_row_per_failure(self, log_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        status = TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            log_path=log_csv,
        )

        assert status == TaskPipelineStatus.COMPLETE
        rows = _read_csv(output)
        assert len(rows) == 2
        assert set(row["task-source"] for row in rows) == {"log"}

    def test_run_log_task_details_json(self, log_csv, tmp_path):
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

    def test_run_log_tasks_empty_when_all_successful(self, tmp_path):
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
        assert status == TaskPipelineStatus.COMPLETE
        assert _read_csv(output) == []

    def test_run_issue_tasks_groups_by_type_and_field(self, issue_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        status = TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            issue_path=issue_csv,
        )

        assert status == TaskPipelineStatus.COMPLETE
        rows = _read_csv(output)
        assert len(rows) == 2
        assert set(row["task-source"] for row in rows) == {"issue"}

    def test_run_issue_task_details_json(self, issue_csv, tmp_path):
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

    def test_run_both_sources_combined(self, log_csv, issue_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        status = TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            log_path=log_csv,
            issue_path=issue_csv,
        )

        assert status == TaskPipelineStatus.COMPLETE
        rows = _read_csv(output)
        sources = set(row["task-source"] for row in rows)
        assert "log" in sources
        assert "issue" in sources

    def test_run_with_config(self, log_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        config = TaskPipelineConfig(
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            output_path=output,
            log_path=log_csv,
        )
        status = TaskPipeline(config).run()
        assert status == TaskPipelineStatus.COMPLETE
        assert len(_read_csv(output)) == 2

    def test_run_output_has_correct_columns(self, log_csv, tmp_path):
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
            "reference",
        }
        rows = _read_csv(output)
        assert set(rows[0].keys()) == expected_cols


RESOURCE_HASH = "be5a869a80edf1eee0cedf66efc726ffe9c51e30f04d4ef976c5b3db4dbb5456"

COLUMN_FIELD_ROWS = (
    "dataset,resource,column,field\n"
    f"tree-preservation-zone,{RESOURCE_HASH},name,name\n"
    f"tree-preservation-zone,{RESOURCE_HASH},geometry,geometry\n"
    f"tree-preservation-zone,{RESOURCE_HASH},reference,reference\n"
)


@pytest.fixture
def column_field_csv(tmp_path):
    path = tmp_path / "column_field.csv"
    path.write_text(COLUMN_FIELD_ROWS)
    return path


class TestColumnFieldTasks:

    def test_missing_mandatory_field_creates_task(self, column_field_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        status = TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=["name", "geometry", "start-date"],
        )

        assert status == TaskPipelineStatus.COMPLETE
        rows = _read_csv(output)
        assert len(rows) == 1
        assert rows[0]["task-source"] == "column-field"
        details = json.loads(rows[0]["details"])
        assert details["field"] == "start-date"
        assert details["issue_type"] == "missing-field"

    def test_multiple_missing_fields_each_produce_a_task(
        self, column_field_csv, tmp_path
    ):
        output = tmp_path / "tasks.csv"
        TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=["name", "start-date", "end-date"],
        )

        rows = _read_csv(output)
        missing_fields = {json.loads(r["details"])["field"] for r in rows}
        assert missing_fields == {"start-date", "end-date"}

    def test_no_tasks_when_all_mandatory_fields_present(
        self, column_field_csv, tmp_path
    ):
        output = tmp_path / "tasks.csv"
        status = TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=["name", "geometry"],
        )

        assert status == TaskPipelineStatus.COMPLETE
        assert _read_csv(output) == []

    def test_alternative_field_group_passes_when_one_present(
        self, column_field_csv, tmp_path
    ):
        """A list inside mandatory_fields means 'any one of these is sufficient'."""
        output = tmp_path / "tasks.csv"
        TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=[["geometry", "point"]],
        )

        assert _read_csv(output) == []

    def test_alternative_field_group_fails_when_none_present(
        self, column_field_csv, tmp_path
    ):
        """All alternatives missing → both are appended as missing."""
        output = tmp_path / "tasks.csv"
        TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=[["start-date", "opening-date"]],
        )

        rows = _read_csv(output)
        missing_fields = {json.loads(r["details"])["field"] for r in rows}
        assert missing_fields == {"start-date", "opening-date"}

    def test_column_field_path_without_mandatory_fields_is_skipped(
        self, column_field_csv, tmp_path, caplog
    ):
        import logging

        output = tmp_path / "tasks.csv"
        with caplog.at_level(logging.WARNING):
            status = TaskPipeline().run(
                output_path=output,
                dataset="tree-preservation-zone",
                organisation="local-authority-eng:ABC",
                endpoint="endpoint-aaa",
                column_field_path=column_field_csv,
            )

        assert status == TaskPipelineStatus.COMPLETE
        assert _read_csv(output) == []
        assert "mandatory_fields not supplied" in caplog.text

    def test_column_field_task_output_columns(self, column_field_csv, tmp_path):
        output = tmp_path / "tasks.csv"
        TaskPipeline().run(
            output_path=output,
            dataset="tree-preservation-zone",
            organisation="local-authority-eng:ABC",
            endpoint="endpoint-aaa",
            column_field_path=column_field_csv,
            mandatory_fields=["start-date"],
        )

        rows = _read_csv(output)
        assert rows[0]["dataset"] == "tree-preservation-zone"
        assert rows[0]["organisation"] == "local-authority-eng:ABC"
        assert rows[0]["severity"] == "error"
        assert rows[0]["responsibility"] == "external"
        assert rows[0]["resource"] == RESOURCE_HASH
        assert rows[0]["reference"] != ""
