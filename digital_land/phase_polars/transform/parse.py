import polars as pl


class ParsePhase:
    """Convert normalised Polars LazyFrame by adding entry numbers."""

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add entry-number column to LazyFrame.

        Args:
            lf: Input Polars LazyFrame

        Returns:
            pl.LazyFrame: LazyFrame with entry-number column
        """
        return lf.with_row_index(name="entry-number", offset=1)
