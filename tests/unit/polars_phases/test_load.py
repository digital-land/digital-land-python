"""Unit tests for PolarsLoadPhase."""

import pytest
import polars as pl
from pathlib import Path
from local_testing.polars_phases.load import PolarsLoadPhase


class TestPolarsLoadPhase:
    """Test suite for PolarsLoadPhase."""

    def test_load_parquet_file(self, tmp_path):
        """Test loading a valid Parquet file."""
        # Create test Parquet file
        test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)

        # Load and verify
        phase = PolarsLoadPhase(str(parquet_path))
        result = phase.process()

        assert result.shape == (3, 5)  # 2 original + 3 metadata columns
        assert "col1" in result.columns
        assert "col2" in result.columns
        assert "priority" in result.columns
        assert "resource" in result.columns
        assert "line-number" in result.columns

    def test_metadata_columns_added(self, tmp_path):
        """Test that metadata columns are added with correct defaults."""
        test_data = pl.DataFrame({"value": [1, 2, 3]})
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)

        phase = PolarsLoadPhase(str(parquet_path))
        result = phase.process()

        assert result["priority"].to_list() == [1, 1, 1]
        assert result["resource"].to_list() == ["", "", ""]
        assert result["line-number"].to_list() == [0, 0, 0]

    def test_metadata_columns_preserved(self, tmp_path):
        """Test that existing metadata columns are not overwritten."""
        test_data = pl.DataFrame(
            {
                "value": [1, 2],
                "priority": [5, 10],
                "resource": ["res1", "res2"],
                "line-number": [100, 200],
            }
        )
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)

        phase = PolarsLoadPhase(str(parquet_path))
        result = phase.process()

        assert result["priority"].to_list() == [5, 10]
        assert result["resource"].to_list() == ["res1", "res2"]
        assert result["line-number"].to_list() == [100, 200]

    def test_non_parquet_file_raises_error(self, tmp_path):
        """Test that non-Parquet files raise ValueError."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1,col2\n1,a\n2,b")

        phase = PolarsLoadPhase(str(csv_path))

        with pytest.raises(ValueError, match="only supports Parquet files"):
            phase.process()

    def test_missing_file_raises_error(self, tmp_path):
        """Test that missing file raises FileNotFoundError."""
        missing_path = tmp_path / "missing.parquet"
        phase = PolarsLoadPhase(str(missing_path))

        with pytest.raises(FileNotFoundError):
            phase.process()

    def test_phase_name(self):
        """Test that phase name is correctly set."""
        phase = PolarsLoadPhase("dummy.parquet")
        assert phase.name == "LoadPhase"

    def test_input_df_ignored(self, tmp_path):
        """Test that input DataFrame parameter is ignored."""
        test_data = pl.DataFrame({"col": [1, 2, 3]})
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)

        phase = PolarsLoadPhase(str(parquet_path))
        input_df = pl.DataFrame({"other": [99]})
        result = phase.process(df=input_df)

        # Should load from file, not use input_df
        assert "col" in result.columns
        assert "other" not in result.columns
