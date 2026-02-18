#!/usr/bin/env python3
"""
Integration test: Convert phase stream -> LazyFrame -> Normalise phase -> Stream
"""
from pathlib import Path
from digital_land.phase.convert import ConvertPhase
from digital_land.phase_polars.transform.normalise import NormalisePhase
from digital_land.utils.convert_stream_polarsdf import StreamToPolarsConverter
import polars as pl


class IntegrationTest:
    def __init__(self):
        test_dir = Path(__file__).parent.parent
        self.csv_path = test_dir / "data" / "Buckinghamshire_Council_sample.csv"
        self.output_dir = test_dir / "data"
        
    def run(self):
        # Read CSV using legacy ConvertPhase
        convert_phase = ConvertPhase(path=str(self.csv_path))
        stream = convert_phase.process()
        
        # Write stream output to text file
        stream_output_file = self.output_dir / "stream_output.txt"
        with open(stream_output_file, 'w') as f:
            for block in stream:
                f.write(str(block) + '\n')
        print(f"Stream output written to: {stream_output_file}")
        
        # Convert Stream to Polars LazyFrame
        convert_phase = ConvertPhase(path=str(self.csv_path))
        stream = convert_phase.process()
        lf = StreamToPolarsConverter.from_stream(stream)
        
        # Pass LazyFrame to normalise phase
        normalise_phase = NormalisePhase()
        lf_normalised = normalise_phase.process(lf)
        
        # Write final LazyFrame output as text
        lazyframe_output_file = self.output_dir / "lazyframe_output.txt"
        df = lf_normalised.collect()
        with open(lazyframe_output_file, 'w') as f:
            f.write(f"\nPolars DataFrame:\n")
            f.write(f"Shape: {df.shape}\n")
            f.write(f"Columns: {df.columns}\n")
            f.write(f"Schema: {df.schema}\n")
            f.write(f"\nAll columns data:\n")
            with pl.Config(set_tbl_cols=-1, set_tbl_rows=-1, set_tbl_width_chars=1000):
                f.write(str(df))
        print(f"LazyFrame output written to: {lazyframe_output_file}")
        
        # Also write as CSV for easier inspection
        csv_output_file = self.output_dir / "normalised_output.csv"
        df.write_csv(csv_output_file)
        print(f"CSV output written to: {csv_output_file}")
        
        print(f"\nProcessed {len(df)} rows with {len(df.columns)} columns")


if __name__ == "__main__":
    test = IntegrationTest()
    test.run()
