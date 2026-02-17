#!/usr/bin/env python3
"""
Integration test: Convert phase stream -> LazyFrame -> Normalise phase
"""
import sys
sys.path.insert(0, '/Users/399182/MHCLG-Github/digital-land-python')

# Mock the missing dependencies
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
from digital_land.utils.convert_dictionary_polarsdf import DictToPolarsConverter
from digital_land.phase_polars.transform.normalise import NormalisePhase

# Path to the CSV file
csv_path = "/Users/399182/MHCLG-Github/digital-land-python/tests/integration/data/Buckinghamshire_Council.csv"

print("=" * 80)
print("STEP 1: Convert phase stream output")
print("=" * 80)

# Create convert phase instance
convert_phase = ConvertPhase(path=csv_path)
stream = convert_phase.process()

# Show first 5 blocks
print("\nFirst 5 blocks from convert phase stream:")
blocks = []
for i, block in enumerate(stream):
    if i >= 5:
        break
    blocks.append(block)
    print(f"\nBlock {i}: line-number={block.get('line-number')}, line={block.get('line')[:3]}...")

print("\n" + "=" * 80)
print("STEP 2: Convert stream to LazyFrame and process through normalise phase")
print("=" * 80)

# Create convert phase instance again (stream is consumed)
convert_phase = ConvertPhase(path=csv_path)
stream = convert_phase.process()

# Convert stream to LazyFrame
print("\nConverting stream to Polars LazyFrame...")
lf = DictToPolarsConverter.from_stream(stream)

print(f"LazyFrame created with {len(lf.columns)} columns")
print(f"Columns: {lf.columns}")

# Collect and show first 5 rows before normalisation
print("\nFirst 5 rows BEFORE normalisation:")
print("-" * 80)
df_before = lf.collect()
print(df_before.head(5))

# Process through normalise phase
print("\nProcessing through NormalisePhase...")
normalise_phase = NormalisePhase()
lf_normalised = normalise_phase.process(lf)

# Collect and show first 5 rows after normalisation
print("\nFirst 5 rows AFTER normalisation:")
print("-" * 80)
df_after = lf_normalised.collect()
print(df_after.head(5))

print("\n" + "=" * 80)
print("Integration test completed successfully!")
print("=" * 80)

print("\n" + "=" * 80)
print("STEP 3: Convert LazyFrame back to stream object")
print("=" * 80)

from digital_land.utils.convert_polarsdf_dictionary import PolarsToDictConverter

# Convert normalized LazyFrame back to stream
print("\nConverting normalized LazyFrame back to stream...")
stream_blocks = PolarsToDictConverter.to_stream_blocks(
    lf_normalised,
    dataset="title-boundary",
    path=csv_path,
    resource="Buckinghamshire_Council"
)

print("\nFirst 5 blocks from converted stream:")
print("-" * 80)
for i, block in enumerate(stream_blocks):
    if i >= 5:
        break
    print(f"\nBlock {i}:")
    print(f"  Keys: {list(block.keys())}")
    print(f"  Dataset: {block.get('dataset')}")
    print(f"  Resource: {block.get('resource')}")
    print(f"  Entry number: {block.get('entry-number')}")
    print(f"  Row (first 3 items): {dict(list(block.get('row', {}).items())[:3])}")
    print("-" * 40)

print("\n" + "=" * 80)
print("Complete workflow: Stream → LazyFrame → Normalise → Stream")
print("=" * 80)

