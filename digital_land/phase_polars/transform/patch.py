import re
import polars as pl


class PatchPhase:
    """Apply regex-based patches to field values using Polars LazyFrame."""

    def __init__(self, patches=None, issues=None):
        self.patch = patches or {}
        self.issues = issues

    def apply_patch(self, fieldname, value):
        patches = {**self.patch.get(fieldname, {}), **self.patch.get("", {})}
        for pattern, replacement in patches.items():
            if pattern == value:
                pattern = f"^{re.escape(pattern)}$"
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if match:
                newvalue = match.expand(replacement)
                if newvalue != value and self.issues:
                    self.issues.log_issue(fieldname, "patch", value)
                return newvalue
        return value

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply patches to LazyFrame columns.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: LazyFrame with patched values
        """
        if not self.patch:
            return lf
        
        df = lf.collect()
        
        for field in df.columns:
            df = df.with_columns(
                pl.col(field).map_elements(
                    lambda val: self.apply_patch(field, val) if val else val,
                    return_dtype=pl.Utf8
                ).alias(field)
            )
        
        return df.lazy()
