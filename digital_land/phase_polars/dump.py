import polars as pl

from .phase import PolarsPhase


class DumpPhase(PolarsPhase):
    """
    Dump raw data to a CSV file (for the ConvertPhase output).
    """

    def __init__(self, path=None, f=None, enabled=True):
        self.path = path
        self.f = f
        self.enabled = enabled

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if not self.enabled or df is None or df.height == 0:
            return df

        data_cols = [c for c in df.columns if not c.startswith("__")]
        out_df = df.select(data_cols)

        if self.f:
            csv_str = out_df.write_csv()
            self.f.write(csv_str)
        elif self.path:
            out_df.write_csv(str(self.path))

        return df
