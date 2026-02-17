import re

import polars as pl

from .phase import PolarsPhase


class PatchPhase(PolarsPhase):
    """
    Apply regex patches to field values.
    """

    def __init__(self, issues=None, patches=None):
        if patches is None:
            patches = {}
        self.issues = issues
        self.patches = patches

    def _apply_patch_value(self, fieldname, value):
        """Apply patch to a single value – mirrors streaming logic exactly."""
        patches = {**self.patches.get(fieldname, {}), **self.patches.get("", {})}
        for pattern, replacement in patches.items():
            original_pattern = pattern
            if pattern == value:
                pattern = f"^{re.escape(pattern)}$"
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if match:
                newvalue = match.expand(replacement)
                if newvalue != value:
                    if self.issues:
                        self.issues.log_issue(fieldname, "patch", value)
                    return newvalue
        return value

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0 or not self.patches:
            return df

        data_cols = [c for c in df.columns if not c.startswith("__")]

        # Determine which fields have patches
        patched_fields = set(self.patches.keys()) - {""}
        global_patches = self.patches.get("", {})

        fields_to_patch = set()
        for col in data_cols:
            if col in patched_fields or global_patches:
                fields_to_patch.add(col)

        if not fields_to_patch:
            return df

        # Use map_elements per field for correctness (regex expand logic is complex)
        for field in fields_to_patch:
            if field not in df.columns:
                continue

            field_patches = {
                **self.patches.get(field, {}),
                **self.patches.get("", {}),
            }
            if not field_patches:
                continue

            def make_patcher(fname, fpatch):
                def _patch(val):
                    if val is None or val == "":
                        return val
                    for pattern, replacement in fpatch.items():
                        p = pattern
                        if p == val:
                            p = f"^{re.escape(p)}$"
                        m = re.match(p, val, flags=re.IGNORECASE)
                        if m:
                            newval = m.expand(replacement)
                            return newval
                    return val
                return _patch

            patcher = make_patcher(field, field_patches)
            df = df.with_columns(
                pl.col(field)
                .map_elements(patcher, return_dtype=pl.Utf8)
                .alias(field)
            )

        return df
