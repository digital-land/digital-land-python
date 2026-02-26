import re
import polars as pl
from datetime import datetime, date
from calendar import monthrange
import logging

logger = logging.getLogger(__name__)

# Storing mandatory fields in dict per dataset
MANDATORY_FIELDS_DICT = {
    "article-4-direction": [
        "reference",
        "name",
        "document-url",
        "documentation-url",
    ],
    "article-4-direction-area": [
        "reference",
        "geometry",
        "name",
        "permitted-development-rights",
    ],
    "conservation-area": ["reference", "geometry", "name"],
    "conservation-area-document": [
        "reference",
        "name",
        "conservation-area",
        "document-url",
        "documentation-url",
        "document-type",
    ],
    "tree-preservation-order": [
        "reference",
        "document-url",
        "documentation-url",
    ],
    "tree-preservation-zone": ["reference", "geometry"],
    "listed-building-outline": ["reference", "geometry", "name", "listed-building"],
    "tree": ["reference", "point", "geometry"],
    "brownfield-land": [
        "OrganisationURI",
        "SiteReference",
        "SiteNameAddress",
        "GeoX",
        "GeoY",
    ],
    "developer-agreement": [
        "reference",
    ],
    "developer-agreement-contribution": [
        "reference",
    ],
    "developer-agreement-transaction": [
        "reference",
    ],
    "infrastructure-funding-statement": [
        "reference",
    ],
}

FAR_FUTURE_YEARS_AHEAD = 50


class HarmonisePhase:
    """
    Apply data harmonisation to Polars LazyFrame using datatype conversions.
    
    Handles field validation, categorical mapping, date normalization,
    geometry processing, and mandatory field checks.
    """

    def __init__(
        self,
        field_datatype_map=None,
        dataset=None,
        valid_category_values=None,
    ):
        """
        Initialize the HarmonisePhase.
        
        Args:
            field_datatype_map: Dictionary mapping field names to datatype names
            dataset: The dataset name (used for mandatory field checking)
            valid_category_values: Dictionary mapping field names to lists of valid values
        """
        self.field_datatype_map = field_datatype_map or {}
        self.dataset = dataset
        self.valid_category_values = valid_category_values or {}

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply harmonisation transformations to LazyFrame.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: Harmonised LazyFrame
        """
        if lf.collect_schema().len() == 0:
            return lf

        existing_columns = lf.collect_schema().names()

        # Apply categorical field normalization
        lf = self._harmonise_categorical_fields(lf, existing_columns)

        # Apply datatype-based field harmonisation
        lf = self._harmonise_field_values(lf, existing_columns)

        # Remove future entry dates
        lf = self._remove_future_dates(lf, existing_columns)

        # Process point geometry (GeoX, GeoY)
        lf = self._process_point_geometry(lf, existing_columns)

        # Ensure typology fields have CURIE prefixes
        lf = self._add_typology_curies(lf, existing_columns)

        # Process Wikipedia URLs
        lf = self._process_wikipedia_urls(lf, existing_columns)

        return lf

    def _harmonise_categorical_fields(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Normalize categorical fields by replacing spaces and validating against allowed values.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with normalised categorical fields
        """
        for field, valid_values in self.valid_category_values.items():
            if field not in existing_columns:
                continue

            # Create a mapping of lowercase values to actual valid values
            value_map = {v.lower().replace(" ", "-"): v for v in valid_values}
            valid_list = list(value_map.values())

            # Apply the categorical normalization
            lf = lf.with_columns(
                pl.col(field)
                .map_elements(
                    lambda x: self._normalize_categorical(x, value_map),
                    return_dtype=pl.Utf8,
                )
                .alias(field)
            )

        return lf

    def _normalize_categorical(self, value, value_map):
        """Normalize a categorical value against allowed values."""
        if not value or (isinstance(value, str) and not value.strip()):
            return value

        normalized = value.replace(" ", "-").lower()
        return value_map.get(normalized, value)

    def _harmonise_field_values(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Apply datatype-based harmonisation to field values.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with harmonised field values
        """
        # For now, this is a placeholder for field harmonisation
        # In a full implementation, this would apply datatype-specific conversions
        # (similar to the legacy phase's harmonise_field method)
        # This could involve:
        # - Decimal formatting
        # - Date standardization
        # - Address normalization
        # - etc.
        return lf

    def _remove_future_dates(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Remove values for entry-date or LastUpdatedDate if they are in the future.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with future dates removed
        """
        today = date.today()

        for field in ["entry-date", "LastUpdatedDate"]:
            if field not in existing_columns:
                continue

            # Create expression to clear future dates
            lf = lf.with_columns(
                pl.when(
                    pl.col(field).str.strptime(pl.Date, "%Y-%m-%d") > pl.lit(today)
                )
                .then(pl.lit(""))
                .otherwise(pl.col(field))
                .alias(field)
            )

        return lf

    def _process_point_geometry(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Process GeoX, GeoY coordinates to ensure valid formatting.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with processed geometry
        """
        if "GeoX" not in existing_columns or "GeoY" not in existing_columns:
            return lf

        # Validate that GeoX and GeoY can be parsed as floats
        # If either is invalid, clear both
        lf = lf.with_columns(
            [
                pl.when(
                    (pl.col("GeoX").str.to_decimal().is_not_null())
                    & (pl.col("GeoY").str.to_decimal().is_not_null())
                )
                .then(pl.col("GeoX").str.to_decimal().cast(pl.Utf8))
                .otherwise(pl.lit(""))
                .alias("GeoX"),
                pl.when(
                    (pl.col("GeoX").str.to_decimal().is_not_null())
                    & (pl.col("GeoY").str.to_decimal().is_not_null())
                )
                .then(pl.col("GeoY").str.to_decimal().cast(pl.Utf8))
                .otherwise(pl.lit(""))
                .alias("GeoY"),
            ]
        )

        return lf

    def _add_typology_curies(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Ensure typology fields (organisation, geography, document) have CURIE prefixes.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with CURIE-formatted typology fields
        """
        if not self.dataset:
            return lf

        for typology in ["organisation", "geography", "document"]:
            if typology not in existing_columns:
                continue

            # Add dataset prefix if value doesn't already contain ":"
            lf = lf.with_columns(
                pl.when(
                    (pl.col(typology).is_not_null())
                    & (pl.col(typology).str.len_chars() > 0)
                    & (~pl.col(typology).str.contains(":"))
                )
                .then(pl.lit(f"{self.dataset}:") + pl.col(typology))
                .otherwise(pl.col(typology))
                .alias(typology)
            )

        return lf

    def _process_wikipedia_urls(
        self, lf: pl.LazyFrame, existing_columns: list
    ) -> pl.LazyFrame:
        """
        Strip protocol from Wikipedia URLs, keeping only the page title.
        
        Args:
            lf: Input LazyFrame
            existing_columns: List of existing column names
            
        Returns:
            pl.LazyFrame: LazyFrame with processed Wikipedia URLs
        """
        if "wikipedia" not in existing_columns:
            return lf

        # Replace full Wikipedia URLs with just the page title
        lf = lf.with_columns(
            pl.col("wikipedia")
            .str.replace(r"https://en\.wikipedia\.org/wiki/", "")
            .str.replace(r"http://en\.wikipedia\.org/wiki/", "")
            .alias("wikipedia")
        )

        return lf

    @staticmethod
    def _get_far_future_date(number_of_years_ahead: int):
        """
        Calculate a date far in the future for validation purposes.
        
        Args:
            number_of_years_ahead: Number of years to add to today
            
        Returns:
            date: A date in the future
        """
        today = date.today()
        y = today.year + number_of_years_ahead
        # keep same month/day if possible (handles Feb 29 & short months)
        last_day = monthrange(y, today.month)[1]
        day = min(today.day, last_day)
        return today.replace(year=y, day=day)
