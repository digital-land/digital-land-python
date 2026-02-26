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
        
        # Process row by row to maintain exact legacy behavior with issue logging
        rows = df.to_dicts()
        for idx, row in enumerate(rows):
            # Set issue context if issues logging is enabled
            if self.issues:
                self.issues.resource = row.get("resource", "")
                self.issues.line_number = row.get("line-number", 0)
                self.issues.entry_number = row.get("entry-number", 0)
            
            # Apply patches to each field in the row
            for field in row:
                if field not in ["resource", "line-number", "entry-number"]:
                    row[field] = self.apply_patch(field, row[field])
        
        return pl.DataFrame(rows).lazy()
