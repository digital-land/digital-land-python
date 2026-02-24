#!/usr/bin/env python3
"""
Integration test: Convert phase stream -> LazyFrame -> Normalise phase -> Stream
"""
import sys
from pathlib import Path

# Mock missing dependencies before imports
class MockUniversalDetector:
    def __init__(self): pass
    def reset(self): pass
    def feed(self, line): pass
    def close(self): pass
    @property
    def done(self): return True
    @property
    def result(self): return {"encoding": "utf-8"}

sys.modules['cchardet'] = type(sys)('cchardet')
sys.modules['cchardet'].UniversalDetector = MockUniversalDetector

from digital_land.phase.convert import ConvertPhase
from digital_land.phase_polars.transform.normalise import NormalisePhase
from digital_land.phase_polars.transform.parse import ParsePhase
from digital_land.phase_polars.transform.concat import ConcatPhase
from digital_land.phase_polars.transform.filter import FilterPhase
from digital_land.phase_polars.transform.map import MapPhase
from digital_land.utils.convert_stream_polarsdf import StreamToPolarsConverter
from digital_land.utils.convert_polarsdf_stream import polars_to_stream
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
        
        # Store original stream blocks
        original_blocks = list(stream)
        
        # Write original stream output
        stream_output_file = self.output_dir / "stream_output.txt"
        with open(stream_output_file, 'w') as f:
            for block in original_blocks:
                f.write(str(block) + '\n')
        print(f"Original stream output written to: {stream_output_file}")
        
        # Convert Stream to Polars LazyFrame
        convert_phase = ConvertPhase(path=str(self.csv_path))
        stream = convert_phase.process()
        lf = StreamToPolarsConverter.from_stream(stream)
        
        # Pass LazyFrame to normalise phase
        normalise_phase = NormalisePhase()
        lf_normalised = normalise_phase.process(lf)
        
        # Pass normalised LazyFrame to parse phase
        parse_phase = ParsePhase()
        lf_parsed = parse_phase.process(lf_normalised)
        
        # Pass parsed LazyFrame to concat phase
        # Test concat configuration: concatenate prefix and reference with "-" separator
        concat_config = {
            "full-reference": {
                "fields": ["prefix", "reference"],
                "separator": "-",
                "prepend": "",
                "append": ""
            }
        }
        concat_phase = ConcatPhase(concats=concat_config)
        lf_concatenated = concat_phase.process(lf_parsed)
        
        # Pass concatenated LazyFrame to filter phase
        # Test filter configuration: only include rows where prefix starts with "title"
        filter_config = {
            "prefix": "^title"
        }
        filter_phase = FilterPhase(filters=filter_config)
        lf_filtered = filter_phase.process(lf_concatenated)
        
        # Pass filtered LazyFrame to map phase
        # Test map configuration: rename columns based on fieldnames
        fieldnames = ["organisation-entity", "reference", "prefix", "full-reference"]
        column_map = {"prefix": "site-prefix"}
        map_phase = MapPhase(fieldnames=fieldnames, columns=column_map)
        lf_mapped = map_phase.process(lf_filtered)
        
        # Write LazyFrame output
        lazyframe_output_file = self.output_dir / "lazyframe_output.txt"
        df = lf_mapped.collect()
        with open(lazyframe_output_file, 'w') as f:
            f.write(f"\nPolars DataFrame:\n")
            f.write(f"Shape: {df.shape}\n")
            f.write(f"Columns: {df.columns}\n")
            f.write(f"Schema: {df.schema}\n")
            f.write(f"\nAll columns data:\n")
            with pl.Config(set_tbl_cols=-1, set_tbl_rows=-1, set_tbl_width_chars=1000):
                f.write(str(df))
        print(f"LazyFrame output written to: {lazyframe_output_file}")
        
        # Convert LazyFrame back to stream
        converted_stream = polars_to_stream(
            lf_mapped,
            dataset="test",
            resource="Buckinghamshire_Council",
            path=str(self.csv_path),
            parsed=False
        )
        converted_blocks = list(converted_stream)
        
        # Write converted stream output
        converted_stream_file = self.output_dir / "converted_stream_output.txt"
        with open(converted_stream_file, 'w') as f:
            for block in converted_blocks:
                f.write(str(block) + '\n')
        print(f"Converted stream output written to: {converted_stream_file}")
        
        # Compare streams
        comparison_file = self.output_dir / "stream_comparison.txt"
        with open(comparison_file, 'w') as f:
            f.write(f"Original stream blocks: {len(original_blocks)}\n")
            f.write(f"Converted stream blocks: {len(converted_blocks)}\n\n")
            
            if len(original_blocks) == len(converted_blocks):
                f.write("Block count matches!\n\n")
                
                # Compare first 3 blocks
                for i in range(min(3, len(original_blocks))):
                    f.write(f"Block {i}:\n")
                    f.write(f"  Original keys: {list(original_blocks[i].keys())}\n")
                    f.write(f"  Converted keys: {list(converted_blocks[i].keys())}\n")
                    
                    if 'line' in original_blocks[i] and 'line' in converted_blocks[i]:
                        orig_line = original_blocks[i]['line']
                        conv_line = converted_blocks[i]['line']
                        f.write(f"  Lines match: {orig_line == conv_line}\n")
                    f.write("\n")
            else:
                f.write("Block count DOES NOT match!\n")
        
        print(f"Stream comparison written to: {comparison_file}")
        
        # Write CSV
        csv_output_file = self.output_dir / "normalised_output.csv"
        df.write_csv(csv_output_file)
        print(f"CSV output written to: {csv_output_file}")
        
        print(f"\nProcessed {len(df)} rows with {len(df.columns)} columns")


if __name__ == "__main__":
    test = IntegrationTest()
    test.run()
