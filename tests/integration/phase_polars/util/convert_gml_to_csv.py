#!/usr/bin/env python3
"""
Convert INSPIRE Index Polygon GML files to CSV using ogr2ogr.

Reads .gml files from:
    tests/integration/data/gml/

Writes converted .csv files to:
    tests/integration/data/csv/

Each GML file is converted with the same ogr2ogr flags used by the project's
ConvertPhase so the output is directly usable by the pipeline benchmark tests.

Usage
-----
    python tests/integration/phase_polars/util/convert_gml_to_csv.py [OPTIONS]

Options
-------
    --input-dir PATH      Override the default GML input directory.
    --output-dir PATH     Override the default CSV output directory.
    --workers INT         Number of parallel conversion processes (default: 4).
    --skip-existing       Skip GML files whose CSV already exists (default: True).
    --council NAME        Convert only files whose stem contains this substring (case-insensitive).
    --dry-run             Print the files that would be converted without converting.
"""

import argparse
import logging
import os
import platform
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from packaging.version import Version

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_INPUT_DIR = (
    Path(__file__).resolve().parents[2] / "data" / "gml"  # tests/integration/
)
DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parents[2] / "data" / "csv"  # tests/integration/
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ogr2ogr helpers  (mirrors digital_land/phase/convert.py)
# ---------------------------------------------------------------------------


def _get_gdal_version() -> Version:
    try:
        out = subprocess.check_output(
            ["ogr2ogr", "--version"], stderr=subprocess.DEVNULL
        ).decode()
        # Accept both "GDAL 3.5.2," (older) and "GDAL 3.12.2 " (newer) formats.
        match = re.search(r"GDAL\s+([0-9]+\.[0-9]+\.[0-9]+)", out)
        if match:
            return Version(match.group(1))
    except Exception:
        pass
    log.warning("Could not detect GDAL version, assuming >= 3.5.2")
    return Version("3.5.2")


def _convert_one(
    gml_path: Path,
    output_dir: Path,
    gdal_version: Version,
    skip_existing: bool,
) -> tuple[str, str]:
    """Convert a single GML file to CSV.

    Returns (stem, status) where status is 'skipped', 'ok', or an error message.
    """
    stem = gml_path.stem
    dest = output_dir / f"{stem}.csv"

    if skip_existing and dest.exists():
        return stem, "skipped"

    command = [
        "ogr2ogr",
        "-oo",
        "DOWNLOAD_SCHEMA=NO",
        "-lco",
        "GEOMETRY=AS_WKT",
        "-lco",
        "GEOMETRY_NAME=WKT",
        "-lco",
        "LINEFORMAT=CRLF",
        "-f",
        "CSV",
        "-nlt",
        "MULTIPOLYGON",
        "-nln",
        "MERGED",
        "--config",
        "OGR_WKT_PRECISION",
        "10",
        str(dest),
        str(gml_path),
    ]

    env = (
        dict(os.environ, OGR_GEOJSON_MAX_OBJ_SIZE="0")
        if gdal_version >= Version("3.5.2")
        else dict(os.environ)
    )

    try:
        result = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return (
                stem,
                f"ERROR: ogr2ogr exited {result.returncode}: {result.stderr.strip()}",
            )
        if not dest.exists():
            return stem, "ERROR: ogr2ogr succeeded but output file not found"
        return stem, "ok"
    except FileNotFoundError:
        return stem, "ERROR: ogr2ogr not found — install GDAL (brew install gdal)"
    except Exception as exc:  # noqa: BLE001
        return stem, f"ERROR: {exc}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert INSPIRE GML files to CSV using ogr2ogr."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing .gml files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write .csv files into (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel conversion processes (default: 4)",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip GML files whose CSV already exists (default: enabled)",
    )
    parser.add_argument(
        "--council",
        type=str,
        default=None,
        help="Convert only files whose stem contains this substring (case-insensitive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print files that would be converted without converting anything",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir

    if not input_dir.is_dir():
        log.error("Input directory not found: %s", input_dir)
        log.error("Run download_inspire_gml.py first to populate it.")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Input  directory: %s", input_dir)
    log.info("Output directory: %s", output_dir)

    gml_files = sorted(input_dir.glob("*.gml"))
    if not gml_files:
        log.error("No .gml files found in %s", input_dir)
        return 1

    log.info("Found %d .gml file(s).", len(gml_files))

    # Apply optional council filter.
    if args.council:
        filter_lower = args.council.lower()
        gml_files = [p for p in gml_files if filter_lower in p.stem.lower()]
        log.info("Filtered to %d file(s) matching %r.", len(gml_files), args.council)

    if not gml_files:
        log.error("No files match the council filter.")
        return 1

    if args.dry_run:
        for p in gml_files:
            print(f"{p}  ->  {output_dir / (p.stem + '.csv')}")
        return 0

    gdal_version = _get_gdal_version()
    log.info("GDAL version: %s", gdal_version)

    ok = skipped = errors = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                _convert_one, gml_path, output_dir, gdal_version, args.skip_existing
            ): gml_path
            for gml_path in gml_files
        }
        total = len(futures)
        done = 0
        for future in as_completed(futures):
            stem, status = future.result()
            done += 1
            if status == "ok":
                ok += 1
                log.info("[%d/%d] ✓ %s", done, total, stem)
            elif status == "skipped":
                skipped += 1
                log.debug("[%d/%d] — skipped %s", done, total, stem)
            else:
                errors += 1
                log.error("[%d/%d] %s  (%s)", done, total, stem, status)

    log.info(
        "Done. converted=%d  skipped=%d  errors=%d  total=%d",
        ok,
        skipped,
        errors,
        total,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
