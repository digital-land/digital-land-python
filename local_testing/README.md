# Digital Land Pipeline - Local Testing

A modular, self-contained environment for testing the digital-land transformation pipeline
on various datasets (e.g., UK Land Registry title-boundary data).

## Architecture

The pipeline uses a **clean modular architecture** with 8 specialized classes:

1. **CLI** (121 lines) - Command-line interface and argument parsing
2. **FileDownloader** (95 lines) - Downloads GML files from data sources
3. **GMLExtractor** (50 lines) - Extracts GML from ZIP archives
4. **GMLConverter** (458 lines) - Converts GML to CSV/Parquet (4 strategies)
5. **PipelineConfig** (93 lines) - Manages pipeline configuration files
6. **PipelineRunner** (254 lines) - Executes 26-phase digital-land transformation
7. **PipelineReport** (346 lines) - Performance tracking and reporting
8. **main.py** (265 lines) - Orchestrates the pipeline by calling specialized classes

**Total**: 2,449 lines across 9 focused, testable modules (down from 1,688 monolithic lines)

## Prerequisites

### Specification Files

The pipeline requires specification files from the digital-land specification repository. These files define schemas, fields, datatypes, and pipeline configurations.

**Files Used (11 of 25):**
- `dataset.csv`, `schema.csv`, `dataset-schema.csv`
- `datatype.csv`, `field.csv`, `dataset-field.csv`, `schema-field.csv`
- `typology.csv`, `pipeline.csv`, `licence.csv`, `provision-rule.csv`

The remaining 14 files are not loaded by the pipeline but may be used by other digital-land tools.

## Quick Start

**Using Makefile (Recommended):**

```bash
# Navigate to directory
cd digital-land-python/local_testing

# First time setup - automatically creates directories, installs dependencies, and clones specification
make init

# If specification already exists elsewhere, you can symlink it instead:
# cd digital-land-python
# ln -s /path/to/your/specification specification

# Verify setup
make check-spec

# List available Local Authorities (for title-boundary dataset)
make list

# Process a specific LA (includes Polars comparison automatically)
make run LA="Buckinghamshire"

# Process with record limit
make run LA="Buckinghamshire" LIMIT=100

# Process ALL Local Authorities (batch mode with comparison)
make run-all

# Process all with record limit (for testing)
make run-all LIMIT=100

# Run only specific phases (e.g., phases 1,2,9)
make run LA="Buckinghamshire" PHASES="1,2,9"

# Run range of phases (e.g., phases 1-5 and 9)
make run LA="Buckinghamshire" PHASES="1-5,9"

# Use best performance (DuckDB + Parquet, includes comparison)
make fast LA="Buckinghamshire"

# See all available commands
make help

# Note: All run commands automatically include Polars comparison
```

**Manual Setup (Alternative):**

```bash
# Navigate to local testing directory
cd digital-land-python/local_testing

# Create virtual environment (first time only)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies (first time only)
pip install polars duckdb

# List available items (dataset-specific)
python main.py --list

# Process a specific item
python main.py --la "Buckinghamshire"

# Process with record limit (for testing)
python main.py --la "Buckinghamshire" --limit 100

# Skip download if already have the file
python main.py --la "Buckinghamshire" --skip-download

# Use DuckDB with Parquet for best performance
python main.py --la "Buckinghamshire" --use-duckdb --use-parquet
```

## What It Does

The pipeline performs 5 steps:

1. **Download** (FileDownloader) - Fetches data files from source API
2. **Extract** (GMLExtractor) - Unzips and locates GML files  
3. **Convert** (GMLConverter) - Parses GML and converts to CSV/Parquet (4 methods available)
4. **Transform** (PipelineRunner) - Runs full 26-phase digital-land pipeline
5. **Report** (PipelineReport) - Generates performance report (JSON + text)

Each step delegates to a specialized class for clean separation of concerns.

## Directory Structure

```
local_testing/
├── main.py              # Main orchestration (265 lines)
├── cli.py               # Command-line interface (121 lines)
├── file_downloader.py   # Downloads GML files (95 lines)
├── gml_extractor.py     # ZIP extraction (50 lines)
├── gml_converter.py     # GML conversion (458 lines)
├── pipeline_config.py   # Config management (93 lines)
├── pipeline_runner.py   # 26-phase transformation (254 lines)
├── pipeline_report.py   # Performance tracking (346 lines)
├── polars_phases.py     # Polars-optimized phases (767 lines)
├── Makefile             # Make commands for easy setup and running
├── README.md            # This file
├── .gitignore           # Git ignore file
├── venv/                # Virtual environment (created with: make init)
├── raw/                 # Downloaded ZIP files
├── extracted/           # Extracted GML files
├── converted/           # GML converted to CSV/Parquet
├── output/              # Pipeline output (harmonised + facts)
├── reports/             # Performance reports
├── cache/               # Organisation.csv cache
├── pipeline/            # Pipeline configuration CSVs
├── specification/       # digital-land specification files
└── scripts/             # Helper scripts
```

## Module Overview

### CLI (`cli.py`)
- Argument parsing with `argparse`
- Fetches endpoint list from GitHub
- Lists and matches data items
- Clean separation of UI logic

### FileDownloader (`file_downloader.py`)
- Downloads files from APIs
- Progress tracking with byte counts
- Reusable for any file download needs

### GMLExtractor (`gml_extractor.py`)
- Extracts GML files from ZIP archives
- Handles nested directory structures
- Simple, focused responsibility

### GMLConverter (`gml_converter.py`)
- **4 conversion strategies**:
  1. Regex → CSV (default, no dependencies)
  2. Regex → Parquet (Polars)
  3. DuckDB → CSV (spatial extension)
  4. DuckDB → Parquet (fastest, best)
- Parses GML polygons to WKT
- Handles coordinate transformation

### PipelineConfig (`pipeline_config.py`)
- Creates pipeline configuration CSVs
- Downloads organization.csv
- Ensures all config files exist

### PipelineRunner (`pipeline_runner.py`)
- Executes 26-phase digital-land pipeline
- Lazy imports for fast startup
- Per-phase timing and metrics
- Handles Parquet/CSV input

### PipelineReport (`pipeline_report.py`)
- Tracks step and phase metrics
- Generates JSON and text reports
- Calculates durations and throughput
- Supports comparison reporting

## Output Files

After running the pipeline, you will find:

**Pipeline Output:**
- `output/{name}_harmonised.csv` - Intermediate harmonised data
- `output/{name}_facts.csv` - Final fact table output  
- `output/{name}_issues.csv` - Any issues logged during processing

**Performance Reports:**

1. **Single LA Report** (default)
   - `reports/{name}_{timestamp}_performance.json` - Detailed JSON report
   - `reports/{name}_{timestamp}_performance.txt` - Human-readable text report
   - Shows timing for all 26 phases

2. **Selective Phase Report** (when using `--phases`)
   - Same format as above
   - Only includes metrics for selected phases
   - Useful for testing specific transformations

3. **Batch Summary Report** (when using `make run-all`)
   - `reports/batch_{timestamp}_summary.json` - Aggregate metrics for entire batch
   - Includes total time, per-LA timing, success/error counts
   - Shows min/max/average processing times across all LAs
   - **All run commands now include automatic Polars comparison** (both Original + Polars pipelines)

## Command Line Options

| Option | Description |
|--------|-------------|
| `--la NAME` | Item name (partial match) |
| `--limit N` | Limit number of records to process |
| `--skip-download` | Use existing downloaded data |
| `--list` | List all available items |
| `--use-duckdb` | Use DuckDB with spatial extension for GML conversion (faster, proper CRS transform) |
| `--use-parquet` | Output Parquet instead of CSV (faster reads, smaller files) |
| `--phases` | Run specific phases (e.g., `1,2,9` or `1-5,9`) |
| `--compare` | Run both original and Polars pipelines for comparison (enabled by default in Makefile) |

## GML Conversion Methods

The **GMLConverter** class supports multiple conversion strategies:

### Output Formats

| Format | Flag | Advantages |
|--------|------|------------|
| **CSV** | (default) | Universal, human-readable |
| **Parquet** | `--use-parquet` | 3-10x smaller, faster reads, preserves types |

### Conversion Engines

| Engine | Flag | Speed | Features |
|--------|------|-------|----------|
| **Regex** | (default) | Slow | No dependencies |
| **DuckDB** | `--use-duckdb` | Fast | Proper CRS transform, spatial extension |

### Best Performance

For the fastest conversion, use DuckDB with Parquet output:

```bash
# Best performance: DuckDB → Parquet
python main.py --la "Buckinghamshire" --use-duckdb --use-parquet
```

## Testing the Modular Architecture

All classes are independently testable:

```bash
# Navigate to directory
cd digital-land-python/local_testing

# Activate venv
source venv/bin/activate

# Verify all modules work
python3 -c "
from cli import CLI
from file_downloader import FileDownloader
from gml_extractor import GMLExtractor
from gml_converter import GMLConverter
from pipeline_config import PipelineConfig
from pipeline_runner import PipelineRunner
from pipeline_report import PipelineReport
print('✅ All modules imported successfully')
"
```

## Notes

- Virtual environment should be created in `local_testing/venv/`
- Entity assignment requires a lookup table (`pipeline/lookup.csv`)
- Without lookups, harmonised data will have empty entity field
- Facts output will be empty without entity lookups
- Coordinates are converted from OSGB (EPSG:27700) to WGS84
- Requirements: `pip install polars duckdb`
- Parquet uses Snappy compression by default
- Add `venv/` to `.gitignore` to avoid committing virtual environment
- **Reusable for other datasets** - Just update the endpoint URL in CLI or main.py

## Development

Each module can be modified independently:

- **Add new conversion method**: Edit `GMLConverter.convert_to_*()` methods
- **Change CLI options**: Edit `CLI.create_parser()`
- **Add new pipeline phases**: Edit `PipelineRunner.run_full_pipeline()`
- **Modify reporting**: Edit `PipelineReport` metrics and output formats
- **Add new data sources**: Create new downloader classes following `FileDownloader` pattern
- **Adapt for new datasets**: Update endpoint URLs and field mappings in relevant classes

The modular structure makes it easy to extend and test each component in isolation.
