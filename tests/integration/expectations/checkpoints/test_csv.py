import csv
import pytest

from digital_land.expectations.checkpoints.csv import CsvCheckpoint


@pytest.fixture
def csv_file(tmp_path):
    file_path = tmp_path / "test.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entity", "name", "reference"])
        writer.writerow(["1", "foo", "ref1"])
        writer.writerow(["2", "bar", "ref2"])
        writer.writerow(["3", "baz", "ref3"])
    return file_path


class TestCsvCheckpoint:
    def test_load_and_run(self, csv_file):
        checkpoint = CsvCheckpoint("test-dataset", csv_file)
        rules = [
            {
                "operation": "count_rows",
                "name": "Row count check",
                "description": "Check CSV has rows",
                "severity": "error",
                "responsibility": "internal",
                "parameters": {"expected": 0, "comparison_rule": "greater_than"},
            }
        ]
        checkpoint.load(rules)
        checkpoint.run()

        assert len(checkpoint.log.entries) == 1
        assert checkpoint.log.entries[0]["passed"] is True
        assert checkpoint.log.entries[0]["operation"] == "count_rows"

    def test_load_and_run_failing(self, csv_file):
        checkpoint = CsvCheckpoint("test-dataset", csv_file)
        rules = [
            {
                "operation": "count_rows",
                "name": "Row count check",
                "parameters": {"expected": 10, "comparison_rule": "equals_to"},
            }
        ]
        checkpoint.load(rules)
        checkpoint.run()

        assert len(checkpoint.log.entries) == 1
        assert checkpoint.log.entries[0]["passed"] is False

    def test_save(self, csv_file, tmp_path):
        checkpoint = CsvCheckpoint("test-dataset", csv_file)
        rules = [
            {
                "operation": "count_rows",
                "name": "Row count check",
                "parameters": {"expected": 0, "comparison_rule": "greater_than"},
            }
        ]
        checkpoint.load(rules)
        checkpoint.run()
        checkpoint.save(tmp_path)

        parquet_path = tmp_path / "dataset=test-dataset" / "test-dataset.parquet"
        assert parquet_path.exists()

    def test_invalid_operation(self, csv_file):
        checkpoint = CsvCheckpoint("test-dataset", csv_file)
        with pytest.raises(ValueError):
            checkpoint.load(
                [{"operation": "nonexistent", "name": "test", "parameters": "{}"}]
            )
