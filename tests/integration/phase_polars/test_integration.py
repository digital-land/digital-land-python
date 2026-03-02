#!/usr/bin/env python3
"""
Integration test: Convert phase stream -> LazyFrame -> polars phases -> Stream -> DefaultPhase (phase 10)

Verifies that HarmonisePhase (polars) can pass its LazyFrame output to the
polars_to_stream utility, which converts it back to a parsed stream, allowing
the legacy DefaultPhase (phase 10) to continue processing.
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
from digital_land.phase.default import DefaultPhase
from digital_land.phase_polars.transform.normalise import NormalisePhase
from digital_land.phase_polars.transform.parse import ParsePhase
from digital_land.phase_polars.transform.concat import ConcatPhase
from digital_land.phase_polars.transform.filter import FilterPhase
from digital_land.phase_polars.transform.map import MapPhase
from digital_land.phase_polars.transform.patch import PatchPhase
from digital_land.phase_polars.transform.harmonise import HarmonisePhase
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
        
        # Pass mapped LazyFrame to patch phase
        # Test patch configuration: normalize site-prefix values
        patch_config = {
            "site-prefix": {
                "^title$": "title-number"
            }
        }
        patch_phase = PatchPhase(patches=patch_config)
        lf_patched = patch_phase.process(lf_mapped)
        
        # Pass patched LazyFrame to harmonise phase
        # Test harmonise configuration with valid category values
        valid_category_values = {}
        harmonise_phase = HarmonisePhase(
            field_datatype_map={},
            dataset="test",
            valid_category_values=valid_category_values
        )
        lf_harmonised = harmonise_phase.process(lf_patched)
        
        # Write LazyFrame output
        lazyframe_output_file = self.output_dir / "lazyframe_output.txt"
        df = lf_harmonised.collect()
        with open(lazyframe_output_file, 'w') as f:
            f.write(f"\nPolars DataFrame:\n")
            f.write(f"Shape: {df.shape}\n")
            f.write(f"Columns: {df.columns}\n")
            f.write(f"Schema: {df.schema}\n")
            f.write(f"\nAll columns data:\n")
            with pl.Config(set_tbl_cols=-1, set_tbl_rows=-1, set_tbl_width_chars=1000):
                f.write(str(df))
        print(f"LazyFrame output written to: {lazyframe_output_file}")
        
        # ── Phase 10: Convert LazyFrame → parsed stream → DefaultPhase ──────────
        # polars_to_stream with parsed=True emits blocks containing a 'row' dict,
        # which is the format expected by every legacy stream-based phase.
        harmonised_stream = polars_to_stream(
            lf_harmonised,
            dataset="test",
            resource="Buckinghamshire_Council",
            path=str(self.csv_path),
            parsed=True,
        )

        # DefaultPhase (phase 10) applies default field values and default values
        # to any empty fields in each row.  For this integration test we run it
        # with empty defaults so it passes every row through unchanged, confirming
        # the stream handoff works correctly.
        default_phase = DefaultPhase(
            default_fields={},
            default_values={},
        )
        default_stream = default_phase.process(harmonised_stream)
        default_blocks = list(default_stream)

        # Write DefaultPhase output
        default_output_file = self.output_dir / "default_phase_output.txt"
        with open(default_output_file, 'w') as f:
            f.write(f"DefaultPhase (phase 10) output\n")
            f.write(f"Blocks processed: {len(default_blocks)}\n\n")
            for block in default_blocks:
                f.write(str(block) + '\n')
        print(f"DefaultPhase output written to: {default_output_file}")

        # Verify the handoff: every block must have the expected stream keys
        assert len(default_blocks) > 0, "DefaultPhase produced no output blocks"
        for block in default_blocks:
            assert "row" in block, f"Missing 'row' key in block: {block}"
            assert "entry-number" in block, f"Missing 'entry-number' key in block: {block}"
        print(f"\nVerification passed: {len(default_blocks)} blocks processed by DefaultPhase")

        # Write CSV (from the harmonised LazyFrame collected earlier)
        csv_output_file = self.output_dir / "normalised_output.csv"
        df.write_csv(csv_output_file)
        print(f"CSV output written to: {csv_output_file}")

        print(f"\nProcessed {len(df)} rows with {len(df.columns)} columns")


if __name__ == "__main__":
    test = IntegrationTest()
    test.run()
