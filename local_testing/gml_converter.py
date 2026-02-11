"""
GML converter for title-boundary datasets.

Provides two conversion strategies:
- DuckDB with spatial extension (fastest, requires network/VPN for GDAL)
- Polars with regex parsing (fallback, works offline/behind firewalls)

All methods output Parquet format for optimal performance.
"""

import os
import re
from pathlib import Path
from typing import Optional

# Configure GDAL to skip network schema resolution (for corporate firewalls/Zscaler)
os.environ['GML_SKIP_RESOLVE_ELEMS'] = 'ALL'
os.environ['GML_SKIP_CORRUPTED_FEATURES'] = 'YES'
os.environ['GDAL_HTTP_TIMEOUT'] = '1'
os.environ['GDAL_HTTP_CONNECTTIMEOUT'] = '1'
os.environ['CPL_DEBUG'] = 'OFF'


class GMLConverter:
    """Converts GML files to Parquet format with multiple strategies."""

    @staticmethod
    def extract_polygon_wkt(geometry_text: str) -> str:
        """
        Extract polygon coordinates and convert to WKT format.
        
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
                    f"{exterior_coords_raw[i]} {exterior_coords_raw[i + 1]}"
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
                    ring_coords.append(f"{coords[i]} {coords[i + 1]}")
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
        Convert GML file to Parquet using Polars (regex-based, works offline).
        
        Fallback method that works behind corporate firewalls/Zscaler.
        Uses regex parsing instead of GDAL, so no network access required.
        
        Args:
            gml_path: Path to input GML file
            parquet_path: Path to output Parquet file
            limit: Optional limit on number of records to convert
            
        Returns:
            Number of records converted
            
        Raises:
            ImportError: If Polars is not installed
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Polars not installed. Install with: pip install polars"
            )
        
        print(f"  Converting GML to Parquet using Polars...")
        print(f"  Input:  {gml_path}")
        print(f"  Output: {parquet_path}")
        
        size_mb = gml_path.stat().st_size / (1024 * 1024)
        print(f"  GML size: {size_mb:.1f} MB")
        
        # Read GML file
        with open(gml_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Extract cadastral parcels using regex
        pattern = r"<LR:PREDEFINED[^>]*>(.*?)</LR:PREDEFINED>"
        matches = re.findall(pattern, content, re.DOTALL)
        
        print(f"  Found {len(matches)} cadastral parcels")
        
        if limit:
            matches = matches[:limit]
            print(f"  Limiting to {limit} records")
        
        # Parse each feature
        records = []
        for parcel_text in matches:
            feature = {
                "reference": self.extract_field(parcel_text, "INSPIREID"),
                "name": self.extract_field(parcel_text, "INSPIREID"),
                "national-cadastral-reference": self.extract_field(
                    parcel_text, "NATIONALCADASTRALREFERENCE"
                ),
                "geometry": self.extract_polygon_wkt(parcel_text),
                "start-date": self.extract_field(parcel_text, "VALIDFROM")[:10]
                if self.extract_field(parcel_text, "VALIDFROM")
                else None,
                "entry-date": self.extract_field(parcel_text, "BEGINLIFESPANVERSION")[
                    :10
                ]
                if self.extract_field(parcel_text, "BEGINLIFESPANVERSION")
                else None,
                "end-date": None,
                "prefix": "title-boundary",
                "organisation": "government-organisation:D2",
                "notes": None,
            }
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

        This method uses DuckDB to directly read GML files and write Parquet output.
        It handles coordinate transformations from EPSG:27700 (OSGB36) to EPSG:4326 (WGS84).

        Args:
            gml_path: Path to input GML file
            parquet_path: Path to output Parquet file
            limit: Optional limit on number of records to convert

        Returns:
            Number of records converted

        Raises:
            ImportError: If DuckDB is not installed
            Exception: If spatial extension cannot be loaded or conversion fails
        """
        import duckdb

        print(f"  Converting GML to Parquet using DuckDB...")
        print(f"  Input:  {gml_path}")
        print(f"  Output: {parquet_path}")

        size_mb = gml_path.stat().st_size / (1024 * 1024)
        print(f"  GML size: {size_mb:.1f} MB")

        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        print("  Loaded DuckDB spatial extension")

        print("  Reading GML file...")
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # GDAL options to skip schema resolution (for corporate firewall/Zscaler)
        gdal_options = ["GML_SKIP_RESOLVE_ELEMS=ALL", "GML_SKIP_CORRUPTED_FEATURES=YES"]
        gdal_opts_str = ", ".join([f"'{opt}'" for opt in gdal_options])

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
            FROM ST_Read('{gml_path}', open_options=[{gdal_opts_str}])
            WHERE INSPIREID IS NOT NULL
            {limit_clause}
        """

        count_query = f"SELECT COUNT(*) FROM ST_Read('{gml_path}', open_options=[{gdal_opts_str}])"
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