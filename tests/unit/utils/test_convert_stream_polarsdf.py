"""Unit tests for StreamToPolarsConverter."""

import polars as pl
from digital_land.utils.convert_stream_polarsdf import StreamToPolarsConverter


class TestStreamToPolarsConverter:
    """Test suite for StreamToPolarsConverter."""

    def test_from_stream_basic(self):
        """Test basic stream to LazyFrame conversion."""
        stream = iter([
            {"line": ["col1", "col2"]},
            {"row": {"col1": "val1", "col2": "val2"}},
            {"row": {"col1": "val3", "col2": "val4"}},
        ])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (2, 2)
        assert df.columns == ["col1", "col2"]
        assert df["col1"].to_list() == ["val1", "val3"]

    def test_from_stream_with_line_blocks(self):
        """Test conversion with line blocks instead of row blocks."""
        stream = iter([
            {"line": ["name", "value"]},
            {"line": ["test1", "100"]},
            {"line": ["test2", "200"]},
        ])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (2, 2)
        assert df["name"].to_list() == ["test1", "test2"]

    def test_from_stream_empty(self):
        """Test empty stream returns empty LazyFrame."""
        stream = iter([])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (0, 0)

    def test_from_stream_header_only(self):
        """Test stream with only header returns empty LazyFrame."""
        stream = iter([{"line": ["col1", "col2"]}])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (0, 0)

    def test_from_stream_missing_fields(self):
        """Test handling of missing fields in row blocks."""
        stream = iter([
            {"line": ["col1", "col2", "col3"]},
            {"row": {"col1": "val1", "col3": "val3"}},
            {"row": {"col1": "val4", "col2": "val5", "col3": "val6"}},
        ])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (2, 3)
        assert df["col2"][0] == ""

    def test_from_stream_skip_invalid_blocks(self):
        """Test that blocks without row or line are skipped."""
        stream = iter([
            {"line": ["col1"]},
            {"row": {"col1": "val1"}},
            {"invalid": "block"},
            {"row": {"col1": "val2"}},
        ])
        
        lf = StreamToPolarsConverter.from_stream(stream)
        df = lf.collect()
        
        assert df.shape == (2, 1)
