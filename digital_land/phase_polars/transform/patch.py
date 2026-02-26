import re
import polars as pl


class PatchPhase:
    """Apply regex-based patches to field values using Polars LazyFrame."""

    def __init__(self, patches=None):
        """
        Initialize the PatchPhase with optional patch rules.
        
        Args:
            patches: Dictionary of patch rules, where keys are field names
                    (or empty string for all fields) and values are dictionaries
                    mapping patterns to their replacement values. Defaults to None.
        """
        self.patch = patches or {}

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply patches to LazyFrame columns using lazy operations.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: LazyFrame with patched values
        """
        if not self.patch:
            return lf
        
        # Iterate through each field in the LazyFrame
        for field in lf.collect_schema().names():
            # Merge field-specific patches with global patches (empty string key)
            field_patches = {**self.patch.get(field, {}), **self.patch.get("", {})}
            
            # Skip this field if no patches are defined for it
            if not field_patches:
                continue
            
            # Start with the original column expression
            col_expr = pl.col(field)
            
            # Apply each pattern-replacement pair as a conditional chain
            for pattern, replacement in field_patches.items():
                # Normalize pattern: if no regex anchor specified, treat as exact match
                if not pattern.startswith("^"):
                    regex_pattern = f"^{re.escape(pattern)}$"
                else:
                    regex_pattern = pattern
                
                # Chain when-then-otherwise conditions for case-insensitive replacement
                col_expr = pl.when(
                    pl.col(field).str.contains(f"(?i){regex_pattern}")
                ).then(
                    pl.col(field).str.replace(f"(?i){regex_pattern}", replacement)
                ).otherwise(col_expr)
            
            # Apply the patched column expression back to the LazyFrame
            lf = lf.with_columns(col_expr.alias(field))
        
        return lf
