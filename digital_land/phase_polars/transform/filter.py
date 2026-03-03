import re
import polars as pl


class FilterPhase:
    """Filter rows based on field values using Polars LazyFrame."""

    def __init__(self, filters=None):
        """
        Initialize filter phase.

        Args:
            filters: Dictionary mapping field names to regex patterns.
                     Only rows where all applicable filters match are included.
        """
        self.filters = {}
        if filters:
            for field, pattern in filters.items():
                self.filters[field] = re.compile(pattern)

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply filter operations to the LazyFrame.

        Args:
            lf: Input Polars LazyFrame

        Returns:
            pl.LazyFrame: Filtered LazyFrame with only matching rows
        """
        if not self.filters:
            return lf

        # Get existing columns
        existing_columns = lf.collect_schema().names()

        # Build filter conditions
        filter_conditions = []

        for field, pattern in self.filters.items():
            # Only apply filter if field exists in the data
            if field in existing_columns:
                # Use str.contains with the pattern
                # Note: re.match() matches from the beginning, so we ensure the pattern
                # is anchored to the start if not already
                pattern_str = pattern.pattern

                # Create a condition that checks if the field matches the pattern
                # Handle null values by treating them as not matching
                condition = pl.col(field).is_not_null() & pl.col(field).str.contains(
                    pattern_str
                )
                filter_conditions.append(condition)

        # Apply all filter conditions with AND logic
        if filter_conditions:
            # Combine all conditions with AND
            combined_condition = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_condition = combined_condition & condition

            lf = lf.filter(combined_condition)

        return lf
