# tests/integration/phase_polars

Integration and performance benchmark tests for the Polars-based pipeline phases
(`digital_land/phase_polars/`), alongside utilities for preparing the test data.

## Directory structure

```
tests/integration/phase_polars/
├── test_integration.py               # Full pipeline integration test
├── test_performance_benchmark.py     # Single-file legacy-vs-Polars benchmark
├── test_performance_benchmark_multi.py  # Multi-file legacy-vs-Polars benchmark
├── test_harmonise_benchmark.py       # HarmonisePhase micro-benchmark
├── load/
│   └── test_load_integration.py      # Load-phase integration tests
├── transform/                        # Unit tests for individual transform phases
│   ├── test_concat_field.py
│   ├── test_map.py
│   ├── test_parse.py
│   ├── test_patch.py
│   └── ...
└── util/                             # Data-preparation scripts (see util/README.md)
    ├── download_inspire_gml.py
    ├── convert_gml_to_csv.py
    └── README.md
```

## Test files

### `test_integration.py`

End-to-end integration test verifying the handoff between the Polars pipeline
and the legacy stream world:

```
ConvertPhase (stream) → Polars phases (Normalise → Parse → Concat → Harmonise)
    → polars_to_stream → legacy DefaultPhase (phase 10)
```

Ensures that data flows correctly through both worlds and that the two
implementations interoperate.

```bash
pytest tests/integration/phase_polars/test_integration.py -v
```

---

### `test_performance_benchmark.py`

Benchmarks **legacy stream phases vs Polars LazyFrame phases** for a **single
CSV file**, phases 1–8 in isolation. Each phase is timed over `N_RUNS`
repetitions; input data is pre-materialised so only the phase's own work is
measured.

**Hardcoded input:** `tests/integration/data/Buckinghamshire_Council.csv`

```bash
python tests/integration/phase_polars/test_performance_benchmark.py
```

Report saved to `tests/integration/data/benchmark_report.txt`.

---

### `test_performance_benchmark_multi.py`

Same benchmark design as above, but scans **all CSV files** in
`tests/integration/data/csv/` and produces:

- A per-file timing table for each council CSV
- An aggregate summary table (sum and avg/file across all files)

```bash
# Run against all available council CSVs
python tests/integration/phase_polars/test_performance_benchmark_multi.py

# Limit to the first 5 files
python tests/integration/phase_polars/test_performance_benchmark_multi.py --files 5

# Use a custom CSV directory
python tests/integration/phase_polars/test_performance_benchmark_multi.py --csv-dir /path/to/csvs
```

| Option | Default | Description |
|---|---|---|
| `--csv-dir PATH` | `tests/integration/data/csv/` | Directory of CSV files to benchmark |
| `--files N` | all | Limit to the first N files |

Report saved to `tests/integration/data/benchmark_report_multi.txt`.

---

### `test_harmonise_benchmark.py`

Micro-benchmark targeting `HarmonisePhase` specifically. Profiles each internal
step independently (categorical fields, field-value normalisation, date
handling, geometry processing) as well as a full end-to-end legacy-vs-Polars
comparison for that phase alone.

```bash
python tests/integration/phase_polars/test_harmonise_benchmark.py
```

---

### `transform/`

Unit tests for individual Polars transform phases (one file per phase):
`ConcatFieldPhase`, `MapPhase`, `ParsePhase`, `PatchPhase`, and others.

```bash
pytest tests/integration/phase_polars/transform/ -v
```

---

### `load/`

Integration tests for the Polars load phases.

```bash
pytest tests/integration/phase_polars/load/ -v
```

---

## Test data

The benchmark scripts require pre-converted INSPIRE GML council data. Use the
scripts in `util/` to prepare it:

```bash
# 1. Download GML files (requires: pip install requests beautifulsoup4)
python tests/integration/phase_polars/util/download_inspire_gml.py --council "Buckinghamshire"

# 2. Convert to CSV (requires: brew install gdal)
python tests/integration/phase_polars/util/convert_gml_to_csv.py --council "Buckinghamshire"

# 3. Run the multi-file benchmark
python tests/integration/phase_polars/test_performance_benchmark_multi.py
```

See [`util/README.md`](util/README.md) for full details on the data-preparation
scripts.

> `tests/integration/data/gml/` and `tests/integration/data/csv/` are excluded
> from version control (large files, several GB for the full dataset).
