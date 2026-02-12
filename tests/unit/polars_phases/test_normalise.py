"""Unit tests for PolarsNormalisePhase."""

import polars as pl
from local_testing.polars_phases.normalise import PolarsNormalisePhase


class TestPolarsNormalisePhase:
    """Test suite for PolarsNormalisePhase."""

    def test_strip_whitespace(self):
        """Test stripping leading and trailing whitespace."""
        df = pl.DataFrame({"col1": ["  value  ", "\ttest\t", " data "]})
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert result["col1"].to_list() == ["value", "test", "data"]

    def test_newline_to_crlf(self):
        """Test converting newlines to CRLF format."""
        df = pl.DataFrame({"col1": ["line1\nline2", "line1\r\nline2", "line1\rline2"]})
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert result["col1"].to_list() == [
            "line1\r\nline2",
            "line1\r\nline2",
            "line1line2",
        ]

    def test_null_patterns_default(self):
        """Test default null patterns are replaced with empty strings."""
        df = pl.DataFrame(
            {"col1": ["null", "NULL", "N/A", "NA", "#N/A", "?", "??", "-", "--"]}
        )
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert all(val == "" for val in result["col1"].to_list())

    def test_null_patterns_custom(self):
        """Test custom null patterns."""
        df = pl.DataFrame(
            {"col1": ["MISSING", "missing", "N/A"], "col2": ["a", "b", "c"]}
        )
        phase = PolarsNormalisePhase(null_patterns=[r"^[Mm][Ii][Ss][Ss][Ii][Nn][Gg]$"])
        result = phase.process(df)

        assert result["col1"].to_list() == ["", "", "N/A"]

    def test_blank_rows_removed(self):
        """Test that blank rows are filtered out."""
        df = pl.DataFrame(
            {"col1": ["value", "", "  ", "data"], "col2": ["a", "", "", "b"]}
        )
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert len(result) == 2
        assert result["col1"].to_list() == ["value", "data"]

    def test_skip_patterns(self):
        """Test rows matching skip patterns are removed."""
        df = pl.DataFrame(
            {"col1": ["keep", "SKIP_THIS", "keep2"], "col2": ["a", "b", "c"]}
        )
        phase = PolarsNormalisePhase(skip_patterns=[r"SKIP"])
        result = phase.process(df)

        assert len(result) == 2
        assert result["col1"].to_list() == ["keep", "keep2"]

    def test_non_string_columns_unchanged(self):
        """Test that non-string columns are not modified."""
        df = pl.DataFrame({"num": [1, 2, 3], "text": ["  a  ", "  b  ", "  c  "]})
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert result["num"].to_list() == [1, 2, 3]
        assert result["text"].to_list() == ["a", "b", "c"]

    def test_phase_name(self):
        """Test that phase name is correctly set."""
        phase = PolarsNormalisePhase()
        assert phase.name == "NormalisePhase"

    def test_empty_dataframe(self):
        """Test processing empty DataFrame."""
        df = pl.DataFrame({"col1": []})
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert len(result) == 0

    def test_null_patterns_from_file(self, tmp_path):
        """Test loading null patterns from CSV file."""
        patterns_file = tmp_path / "patterns.csv"
        patterns_df = pl.DataFrame({"pattern": [r"^CUSTOM$", r"^TEST$"]})
        patterns_df.write_csv(patterns_file)

        df = pl.DataFrame({"col1": ["CUSTOM", "TEST", "null"], "col2": ["a", "b", "c"]})
        phase = PolarsNormalisePhase(null_patterns_path=str(patterns_file))
        result = phase.process(df)

        assert result["col1"].to_list() == ["", "", "null"]

    def test_null_patterns_file_fallback(self):
        """Test fallback to default patterns when file doesn't exist."""
        df = pl.DataFrame({"col1": ["null", "N/A"]})
        phase = PolarsNormalisePhase(null_patterns_path="nonexistent.csv")
        result = phase.process(df)

        assert all(val == "" for val in result["col1"].to_list())

    def test_multiple_skip_patterns(self):
        """Test multiple skip patterns."""
        df = pl.DataFrame({"col1": ["keep", "SKIP1", "SKIP2", "keep2"]})
        phase = PolarsNormalisePhase(skip_patterns=[r"SKIP1", r"SKIP2"])
        result = phase.process(df)

        assert len(result) == 2
        assert result["col1"].to_list() == ["keep", "keep2"]

    def test_combined_normalization(self):
        """Test all normalization operations together."""
        df = pl.DataFrame(
            {
                "col1": ["  value  ", "null", "  ", "data\nline2"],
                "col2": ["a", "N/A", "", "b"],
            }
        )
        phase = PolarsNormalisePhase()
        result = phase.process(df)

        assert len(result) == 2
        assert result["col1"].to_list() == ["value", "data\r\nline2"]
        assert result["col2"].to_list() == ["a", "b"]
