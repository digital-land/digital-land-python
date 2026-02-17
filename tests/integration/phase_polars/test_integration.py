#!/usr/bin/env python3
"""
Script to demonstrate the output of convert phase stream
"""
from digital_land.phase.convert import ConvertPhase

# Path to the CSV file
csv_path = "/Users/399182/MHCLG-Github/digital-land-python/tests/integration/data/Buckinghamshire_Council.csv"

# Create convert phase instance
convert_phase = ConvertPhase(path=csv_path)

# Process the file
stream = convert_phase.process()

# Print first 5 blocks from the stream
print("First 5 blocks from convert phase stream:\n")
print("=" * 80)

for i, block in enumerate(stream):
    if i >= 5:
        break
    print(f"\nBlock {i}:")
    print(f"  Keys: {list(block.keys())}")
    print(f"  Dataset: {block.get('dataset')}")
    print(f"  Resource: {block.get('resource')}")
    print(f"  Line number: {block.get('line-number')}")
    print(f"  Line (first 100 chars): {str(block.get('line'))[:100]}...")
    print(f"  Row: {block.get('row')}")
    print("-" * 80)

print("\nDone!")
