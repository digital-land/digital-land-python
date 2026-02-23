import polars as pl


class ConcatPhase:
    """Concatenate fields using Polars LazyFrame."""

    def __init__(self, concats=None, log=None):
        """
        Initialize concat phase.
        
        Args:
            concats: Dictionary mapping field names to concatenation specs.
                     Each spec contains:
                     - fields: list of field names to concatenate
                     - separator: string to join fields
                     - prepend: optional string to prepend (default: "")
                     - append: optional string to append (default: "")
            log: Optional column field log for tracking operations
        """
        self.concats = concats or {}
        
        if log:
            for fieldname, cat in self.concats.items():
                log.add(
                    fieldname,
                    cat["prepend"]
                    + cat["separator"].join(cat["fields"])
                    + cat["append"],
                )

    def process(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply concatenation operations to the LazyFrame.
        
        Args:
            lf: Input Polars LazyFrame
            
        Returns:
            pl.LazyFrame: LazyFrame with concatenated fields
        """
        if not self.concats:
            return lf
        
        # Build list of column expressions
        exprs = []
        existing_columns = lf.collect_schema().names()
        
        for fieldname, cat in self.concats.items():
            separator = cat["separator"]
            source_fields = cat["fields"]
            prepend = cat.get("prepend", "")
            append = cat.get("append", "")
            
            # Build list of field expressions to concatenate
            field_exprs = []
            
            # Include existing field value if it exists and is not empty
            if fieldname in existing_columns:
                field_exprs.append(
                    pl.when(
                        (pl.col(fieldname).is_not_null()
                         & (pl.col(fieldname).str.strip_chars() != ""))
                    )
                    .then(pl.col(fieldname))
                    .otherwise(pl.lit(None))
                )
            
            # Add source fields that exist and are not empty
            for field in source_fields:
                if field in existing_columns:
                    field_exprs.append(
                        pl.when(
                            (pl.col(field).is_not_null()
                             & (pl.col(field).str.strip_chars() != ""))
                        )
                        .then(pl.col(field))
                        .otherwise(pl.lit(None))
                    )
            
            # Concatenate all non-null field values
            if field_exprs:
                # Use concat_list to combine all fields, then drop nulls, then join
                concat_expr = (
                    pl.concat_list(field_exprs)
                    .list.drop_nulls()
                    .list.join(separator)
                )
                
                # Add prepend and append if specified
                if prepend or append:
                    concat_expr = pl.concat_str([
                        pl.lit(prepend),
                        concat_expr,
                        pl.lit(append)
                    ])
                
                exprs.append(concat_expr.alias(fieldname))
            else:
                # If no fields to concatenate, just use prepend + append
                exprs.append(pl.lit(prepend + append).alias(fieldname))
        
        # Apply all concat expressions
        if exprs:
            lf = lf.with_columns(exprs)
        
        return lf
