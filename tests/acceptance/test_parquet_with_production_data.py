"""
Acceptance tests for parquet file creation with real production data.

These tests use actual production data from planning.data.gov.uk to ensure
the parquet functionality works with real-world, large-scale datasets.
"""

import pytest
import urllib.request
import duckdb

from digital_land.commands import _create_parquet_from_csv
from digital_land.specification import Specification
import csv


@pytest.mark.slow
def test_dataset_dump_flattened_with_border_dataset(tmp_path, specification_dir):
    """
    Test parquet creation with the border dataset which contains very large geometry columns.

    The border.csv file from production contains extremely large MULTIPOLYGON geometries
    (the file is ~17MB), making it an excellent test for VARCHAR size limits and
    large field handling.
    """

    # Download the production border.csv file
    csv_url = "https://files.planning.data.gov.uk/dataset/border.csv"
    csv_path = tmp_path / "border.csv"

    print(f"Downloading border.csv from {csv_url}...")
    urllib.request.urlretrieve(csv_url, csv_path)

    # Verify the file was downloaded
    assert csv_path.exists(), "Border CSV should be downloaded"
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    print(f"Downloaded border.csv: {file_size_mb:.2f}MB")

    # Get field names from CSV
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        field_names = reader.fieldnames

    print(f"Field names: {field_names}")

    # Create output directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Load specification
    specification = Specification(specification_dir)
    dataset_name = "border"

    # Execute _create_parquet_from_csv directly
    print("Creating parquet file from production border.csv...")
    _create_parquet_from_csv(
        str(csv_path), str(output_dir), dataset_name, specification, field_names
    )

    # Assert parquet file was created
    parquet_path = output_dir / "border.parquet"
    assert parquet_path.exists(), "Parquet file should be created"

    # Verify parquet file contains data
    conn = duckdb.connect()

    # Get row count
    row_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]

    print(f"Parquet file contains {row_count} rows")
    assert row_count > 0, "Parquet file should contain data"

    # Verify we can read the geometry column and it contains large geometries
    result = conn.execute(
        f"""
        SELECT
            entity,
            length(geometry) as geom_length
        FROM read_parquet('{parquet_path}')
        WHERE geometry IS NOT NULL
        ORDER BY length(geometry) DESC
        LIMIT 1
    """
    ).fetchone()

    if result:
        entity, geom_length = result
        print(f"Largest geometry: Entity {entity}, Length {geom_length:,} bytes")

        # The border dataset has geometries that are several MB in size
        assert geom_length > 100000, "Should contain large geometries (>100KB)"

    # Verify schema has correct types
    schema = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
    ).fetchall()

    schema_dict = {row[0]: row[1] for row in schema}

    # Check key fields have correct types
    if "entity" in schema_dict:
        assert "BIGINT" in schema_dict["entity"], "Entity should be BIGINT"

    if "geometry" in schema_dict:
        assert "VARCHAR" in schema_dict["geometry"], "Geometry should be VARCHAR"

    if "name" in schema_dict:
        assert "VARCHAR" in schema_dict["name"], "Name should be VARCHAR"

    conn.close()

    print("✓ Successfully created parquet from 17MB production border dataset")
    print(f"  Parquet file size: {(parquet_path.stat().st_size / (1024 * 1024)):.2f}MB")


@pytest.mark.slow
def test_parquet_compression_efficiency(tmp_path, specification_dir):
    """
    Test that parquet compression (ZSTD) provides good compression ratios
    compared to the original CSV, especially for large datasets.
    """
    # Download the production border.csv file
    csv_url = "https://files.planning.data.gov.uk/dataset/border.csv"
    csv_path = tmp_path / "border.csv"

    print("Downloading border.csv...")
    urllib.request.urlretrieve(csv_url, csv_path)

    csv_size_mb = csv_path.stat().st_size / (1024 * 1024)

    # Get field names
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        field_names = reader.fieldnames

    # Create output directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    specification = Specification(specification_dir)

    _create_parquet_from_csv(
        str(csv_path), str(output_dir), "border", specification, field_names
    )

    # Compare sizes
    parquet_path = output_dir / "border.parquet"
    parquet_size_mb = parquet_path.stat().st_size / (1024 * 1024)

    compression_ratio = csv_size_mb / parquet_size_mb

    print(f"Original CSV: {csv_size_mb:.2f}MB")
    print(f"Parquet (ZSTD): {parquet_size_mb:.2f}MB")
    print(f"Compression ratio: {compression_ratio:.2f}x")

    # Parquet with ZSTD should provide some compression
    assert parquet_size_mb < csv_size_mb, "Parquet should be smaller than CSV"

    print(f"✓ Parquet is {compression_ratio:.2f}x smaller than CSV")
