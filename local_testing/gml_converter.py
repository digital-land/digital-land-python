"""
GML converter for title-boundary datasets.

Provides Parquet conversion strategies optimized for performance:
- Polars-based Parquet conversion (regex parsing, good compatibility)
- DuckDB-based Parquet conversion (fastest, with spatial transforms)

All methods output Parquet format for optimal performance with the Polars pipeline.
"""

import re
from pathlib import Path
from typing import Optional


class GMLConverter:
    """Converts GML files to Parquet format with multiple strategies."""

    @staticmethod
    def extract_polygon_wkt(geometry_text: str) -> str:
        """
        Extract polygon coordinates and convert to WKT format.

        Handles both exterior rings and interior rings (holes).

        Args:
            geometry_text: GML geometry element text

        Returns:
            WKT polygon string, or empty string if no valid geometry
        """
        exterior_match = re.search(
            r"<gml:exterior>.*?<gml:posList>([^<]+)</gml:posList>.*?</gml:exterior>",
            geometry_text,
            re.DOTALL,
        )

        if not exterior_match:
            return ""

        exterior_coords_raw = exterior_match.group(1).strip().split()
        exterior_coords = []
        for i in range(0, len(exterior_coords_raw), 2):
            if i + 1 < len(exterior_coords_raw):
                exterior_coords.append(
                    f"{exterior_coords_raw[i]} {exterior_coords_raw[i+1]}"
                )

        if not exterior_coords:
            return ""

        # Extract interior rings (holes)
        interior_rings = []
        interior_matches = re.findall(
            r"<gml:interior>.*?<gml:posList>([^<]+)</gml:posList>.*?</gml:interior>",
            geometry_text,
            re.DOTALL,
        )

        for interior_coords_raw in interior_matches:
            coords = interior_coords_raw.strip().split()
            ring_coords = []
            for i in range(0, len(coords), 2):
                if i + 1 < len(coords):
                    ring_coords.append(f"{coords[i]} {coords[i+1]}")
            if ring_coords:
                interior_rings.append(ring_coords)

        exterior_wkt = f"({', '.join(exterior_coords)})"
        if interior_rings:
            interior_wkts = [f"({', '.join(ring)})" for ring in interior_rings]
            return f"POLYGON({exterior_wkt}, {', '.join(interior_wkts)})"
        return f"POLYGON({exterior_wkt})"

    @staticmethod
    def extract_field(text: str, field_name: str) -> str:
        """
        Extract a field value from GML text.

        Args:
            text: GML text to search
            field_name: Field name to extract

        Returns:
            Field value, or empty string if not found
        """
        pattern = f"<LR:{field_name}>([^<]+)</LR:{field_name}>"
        match = re.search(pattern, text)
        return match.group(1) if match else ""



    def convert_to_parquet(
        self, gml_path: Path, parquet_path: Path, limit: Optional[int] = None
    ) -> int:
        """
        Convert GML file to Parquet format using regex parsing + Polars.

        Parquet preserves data types and is optimized for the Polars pipeline.
        Requires Polars to be installed.

        Args:
            gml_path: Path to input GML file
            parquet_path: Path to output Parquet file
            limit: Optional limit on number of records to convert

        Returns:
            Number of records converted
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Polars is required for GML conversion. Install with: pip install polars"
            )

        print(f"  Converting GML to Parquet...")
        print(f"  Input:  {gml_path}")
        print(f"  Output: {parquet_path}")

        size_mb = gml_path.stat().st_size / (1024 * 1024)
        print(f"  GML size: {size_mb:.1f} MB")

        with open(gml_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Find all cadastral parcel elements
        pattern = r"<LR:PREDEFINED[^>]*>(.*?)</LR:PREDEFINED>"
        matches = re.findall(pattern, content, re.DOTALL)
        total_features = len(matches)
        print(f"  Found {total_features} cadastral parcels")

        if limit:
            print(f"  Limiting to {limit} records")
            matches = matches[:limit]

        # Build list of records
        records = []
        for match in matches:
            feature = {}

            inspire_id = self.extract_field(match, "INSPIREID")
            if inspire_id:
                feature["reference"] = inspire_id
                feature["name"] = inspire_id

            ncr = self.extract_field(match, "NATIONALCADASTRALREFERENCE")
            if ncr:
                feature["national-cadastral-reference"] = ncr

            valid_from = self.extract_field(match, "VALIDFROM")
            if valid_from:
                feature["start-date"] = (
                    valid_from.split("T")[0] if "T" in valid_from else valid_from
                )

            begin_lifespan = self.extract_field(match, "BEGINLIFESPANVERSION")
            if begin_lifespan:
                feature["entry-date"] = (
                    begin_lifespan.split("T")[0]
                    if "T" in begin_lifespan
                    else begin_lifespan
                )

            geometry_match = re.search(
                r"<LR:GEOMETRY>(.*?)</LR:GEOMETRY>", match, re.DOTALL
            )
            if geometry_match:
                wkt = self.extract_polygon_wkt(geometry_match.group(1))
                if wkt:
                    feature["geometry"] = wkt

            if "reference" in feature:
                feature["prefix"] = "title-boundary"
                feature["organisation"] = "government-organisation:D2"
                feature["end-date"] = None
                feature["notes"] = None
                records.append(feature)

        # Create DataFrame and write to Parquet
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        df = pl.DataFrame(records)
        df.write_parquet(parquet_path, compression="snappy")

        count = len(records)
        print(f"  Converted {count} records to Parquet")
        return count

    def convert_to_parquet_duckdb(
        self, gml_path: Path, parquet_path: Path, limit: Optional[int] = None
    ) -> int:
        """
        Convert GML file to Parquet format using DuckDB with spatial extension.

        This is the fastest method - DuckDB reads GML directly and writes Parquet.
        Falls back to Polars-based converter if DuckDB is not available.

        Args:
            gml_path: Path to input GML file
            parquet_path: Path to output Parquet file
            limit: Optional limit on number of records to convert

        Returns:
            Number of records converted
        """
        try:
            import duckdb
        except ImportError:
            print("  DuckDB not installed. Install with: pip install duckdb")
            print("  Falling back to Polars-based converter...")
            return self.convert_to_parquet(gml_path, parquet_path, limit)

        print(f"  Converting GML to Parquet using DuckDB...")
        print(f"  Input:  {gml_path}")
        print(f"  Output: {parquet_path}")

        size_mb = gml_path.stat().st_size / (1024 * 1024)
        print(f"  GML size: {size_mb:.1f} MB")

        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial; LOAD spatial;")
                print("  Loaded DuckDB spatial extension")
            except Exception as ext_err:
                print(f"  Failed to load spatial extension: {ext_err}")
                print("  Falling back to Polars-based converter...")
                con.close()
                return self.convert_to_parquet(gml_path, parquet_path, limit)

            print("  Reading GML file...")
            limit_clause = f"LIMIT {limit}" if limit else ""

            query = f"""
                SELECT
                    INSPIREID as reference,
                    INSPIREID as name,
                    NATIONALCADASTRALREFERENCE as "national-cadastral-reference",
                    ST_AsText(ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')) as geometry,
                    CASE
                        WHEN VALIDFROM IS NOT NULL
                        THEN strftime(CAST(VALIDFROM AS DATE), '%Y-%m-%d')
                        ELSE NULL
                    END as "start-date",
                    CASE
                        WHEN BEGINLIFESPANVERSION IS NOT NULL
                        THEN strftime(CAST(BEGINLIFESPANVERSION AS DATE), '%Y-%m-%d')
                        ELSE NULL
                    END as "entry-date",
                    NULL as "end-date",
                    'title-boundary' as prefix,
                    'government-organisation:D2' as organisation,
                    NULL as notes
                FROM ST_Read('{gml_path}')
                WHERE INSPIREID IS NOT NULL
                {limit_clause}
            """

            count_query = f"SELECT COUNT(*) FROM ST_Read('{gml_path}')"
            total_count = con.execute(count_query).fetchone()[0]
            print(f"  Found {total_count:,} cadastral parcels")

            if limit:
                print(f"  Limiting to {limit} records")

            # Export directly to Parquet (much faster than CSV)
            print("  Transforming and writing to Parquet...")
            con.execute(
                f"COPY ({query}) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'snappy')"
            )

            # Count output rows
            result_count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
            ).fetchone()[0]

            con.close()

            print(f"  Converted {result_count:,} records to Parquet")
            return result_count

        except Exception as e:
            print(f"  DuckDB conversion failed: {e}")
            print("  Falling back to Polars-based converter...")
            return self.convert_to_parquet(gml_path, parquet_path, limit)


