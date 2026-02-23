"""Unit tests for polars_to_stream."""

import polars as pl
from digital_land.utils.convert_polarsdf_stream import polars_to_stream


class TestPolarsToStream:
    """Test suite for polars_to_stream."""

    def test_polars_to_stream_unparsed(self):
        """Test LazyFrame to stream conversion in unparsed format."""
        df = pl.DataFrame({"col1": ["val1", "val2"], "col2": ["a", "b"]})
        lf = df.lazy()

        blocks = list(
            polars_to_stream(
                lf, dataset="test", resource="res1", path="/test.csv", parsed=False
            )
        )

        assert len(blocks) == 3
        assert blocks[0]["line"] == ["col1", "col2"]
        assert blocks[0]["line-number"] == 0
        assert blocks[1]["line"] == ["val1", "a"]
        assert blocks[1]["line-number"] == 1

    def test_polars_to_stream_parsed(self):
        """Test LazyFrame to stream conversion in parsed format."""
        df = pl.DataFrame({"name": ["test1", "test2"], "value": [100, 200]})
        lf = df.lazy()

        blocks = list(
            polars_to_stream(
                lf, dataset="test", resource="res1", path="/test.csv", parsed=True
            )
        )

        assert len(blocks) == 2
        assert blocks[0]["entry-number"] == 1
        assert blocks[0]["row"] == {"name": "test1", "value": 100}
        assert blocks[1]["entry-number"] == 2
        assert blocks[1]["row"] == {"name": "test2", "value": 200}

    def test_polars_to_stream_empty(self):
        """Test empty LazyFrame conversion."""
        df = pl.DataFrame({"col1": []})
        lf = df.lazy()

        blocks = list(polars_to_stream(lf, parsed=False))

        assert len(blocks) == 1
        assert blocks[0]["line"] == ["col1"]

    def test_polars_to_stream_metadata(self):
        """Test that metadata is correctly included."""
        df = pl.DataFrame({"col": ["val"]})
        lf = df.lazy()

        blocks = list(
            polars_to_stream(
                lf, dataset="ds1", resource="r1", path="/p.csv", parsed=False
            )
        )

        assert blocks[0]["dataset"] == "ds1"
        assert blocks[0]["resource"] == "r1"
        assert blocks[0]["path"] == "/p.csv"
