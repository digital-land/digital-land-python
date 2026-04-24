# tests/integration/phase_polars/util

Utility scripts for preparing test data used by the phase_polars integration and
performance benchmark tests.

## Scripts

### `download_inspire_gml.py`

Downloads INSPIRE Index Polygon GML files from the
[HM Land Registry download service](https://use-land-property-data.service.gov.uk/datasets/inspire/download).

Each council entry on that page links to a ZIP archive containing a single `.gml`
file. The script downloads and extracts them to `tests/integration/data/gml/`,
naming each file after the council (e.g. `Buckinghamshire_Council.gml`) so that
multiple councils can coexist and `--skip-existing` works correctly.

**Prerequisites:** `pip install requests beautifulsoup4`

```bash
# Download all councils (slow – 318 files)
python tests/integration/phase_polars/util/download_inspire_gml.py

# Download a single council
python tests/integration/phase_polars/util/download_inspire_gml.py --council "Buckinghamshire"

# Preview download URLs without downloading anything
python tests/integration/phase_polars/util/download_inspire_gml.py --dry-run
```

| Option | Default | Description |
|---|---|---|
| `--output-dir PATH` | `tests/integration/data/gml/` | Directory to write GML files into |
| `--workers INT` | `4` | Parallel download threads |
| `--skip-existing` / `--no-skip-existing` | enabled | Skip councils already downloaded |
| `--council NAME` | all | Substring filter on council name |
| `--dry-run` | off | Print URLs without downloading |

---

### `convert_gml_to_csv.py`

Converts `.gml` files in `tests/integration/data/gml/` to CSV using `ogr2ogr`,
writing the results to `tests/integration/data/csv/`. The same `ogr2ogr` flags
used by the project's `ConvertPhase` are applied (`GEOMETRY=AS_WKT`,
`MULTIPOLYGON`, WKT precision 10), so the output is directly usable by the
pipeline benchmark and harmonise tests.

**Prerequisites:** GDAL must be installed and `ogr2ogr` on `PATH`
(`brew install gdal` on macOS).

```bash
# Convert all downloaded GML files
python tests/integration/phase_polars/util/convert_gml_to_csv.py

# Convert a single council
python tests/integration/phase_polars/util/convert_gml_to_csv.py --council "Buckinghamshire"

# Preview what would be converted without converting
python tests/integration/phase_polars/util/convert_gml_to_csv.py --dry-run
```

| Option | Default | Description |
|---|---|---|
| `--input-dir PATH` | `tests/integration/data/gml/` | Directory containing `.gml` files |
| `--output-dir PATH` | `tests/integration/data/csv/` | Directory to write `.csv` files into |
| `--workers INT` | `4` | Parallel conversion processes |
| `--skip-existing` / `--no-skip-existing` | enabled | Skip files whose CSV already exists |
| `--council NAME` | all | Substring filter on GML filename stem |
| `--dry-run` | off | Print conversions without running them |

Converted CSV columns: `WKT, gml_id, INSPIREID, LABEL, NATIONALCADASTRALREFERENCE, VALIDFROM, BEGINLIFESPANVERSION`

---

## Typical workflow

```bash
# 1. Download GML files for all (or specific) councils
python tests/integration/phase_polars/util/download_inspire_gml.py --council "Buckinghamshire"

# 2. Convert to CSV
python tests/integration/phase_polars/util/convert_gml_to_csv.py --council "Buckinghamshire"

# 3. Run the benchmark tests
python tests/integration/phase_polars/test_performance_benchmark.py
```

## Output directories

| Directory | Contents |
|---|---|
| `tests/integration/data/gml/` | Raw `.gml` files, one per council |
| `tests/integration/data/csv/` | Converted `.csv` files, one per council |

> Both directories are excluded from version control via `.gitignore` as the
> files are large (the full dataset is several GB).
