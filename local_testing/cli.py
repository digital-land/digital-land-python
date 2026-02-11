"""
Command-line interface for title-boundary pipeline.

Handles argument parsing and provides user-facing CLI functions.
"""

import argparse
from typing import List, Dict

from file_downloader import FileDownloader


class CLI:
    """Command-line interface manager."""

    ENDPOINT_CSV_URL = "https://raw.githubusercontent.com/digital-land/config/main/collection/title-boundary/endpoint.csv"

    @staticmethod
    def create_parser() -> argparse.ArgumentParser:
        """
        Create argument parser for CLI.

        Returns:
            Configured ArgumentParser instance
        """
        parser = argparse.ArgumentParser(
            description="Title Boundary Pipeline - Download, Convert, and Transform",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  python main.py                            # List available LAs
  python main.py --la "Buckinghamshire"     # Process Buckinghamshire (Polars+Parquet)
  python main.py --la "Buckinghamshire" --limit 100   # Limit to 100 records
  python main.py --use-duckdb               # Use DuckDB for conversion (faster)
  python main.py --compare                  # Run both pipelines for comparison
            """,
        )

        parser.add_argument(
            "--la", type=str, help="Local Authority name (partial match)"
        )
        parser.add_argument(
            "--limit", type=int, help="Limit number of records to process"
        )
        parser.add_argument(
            "--skip-download",
            action="store_true",
            help="Skip download, use existing data",
        )
        parser.add_argument(
            "--list", action="store_true", help="List available Local Authorities"
        )
        parser.add_argument(
            "--use-duckdb",
            action="store_true",
            help="Use DuckDB for GML conversion (faster, with coordinate transforms)",
        )
        parser.add_argument(
            "--compare",
            action="store_true",
            help="Run both original and Polars pipelines for performance comparison",
        )
        parser.add_argument(
            "--phases",
            type=str,
            help="Comma-separated phase numbers to run (e.g. '1,2,9' or '1-5,9')",
        )

        return parser

    @classmethod
    def fetch_endpoint_list(cls) -> List[Dict]:
        """
        Fetch list of available endpoints from Land Registry API.

        Returns:
            List of endpoint dictionaries
        """
        return FileDownloader().fetch_endpoint_list()

    @staticmethod
    def get_la_name_from_url(url: str) -> str:
        """
        Extract Local Authority name from endpoint URL.

        Args:
            url: Endpoint URL

        Returns:
            Formatted LA name
        """
        return FileDownloader.get_la_name_from_url(url)

    @classmethod
    def list_available_las(cls):
        """List all available Local Authorities to console."""
        endpoints = cls.fetch_endpoint_list()

        print(f"\n{'='*60}")
        print("Available Local Authorities")
        print(f"{'='*60}\n")

        for i, ep in enumerate(endpoints, 1):
            name = ep.get("local_authority", "Unknown")
            print(f"  {i:3d}. {name}")

        print(f"\n{'='*60}")
        print(f"Total: {len(endpoints)} Local Authorities")
        print(f"{'='*60}\n")

        return endpoints

    @classmethod
    def find_matching_la(cls, search_term: str) -> tuple:
        """
        Find Local Authority matching search term.

        Args:
            search_term: Partial LA name to search for

        Returns:
            Tuple of (matching_endpoint, la_name) or (None, None) if no match/multiple matches
        """
        endpoints = cls.fetch_endpoint_list()
        matching = [
            ep
            for ep in endpoints
            if search_term.lower() in ep.get("local_authority", "").lower()
        ]

        if not matching:
            print(f"Error: No Local Authority matching '{search_term}'")
            print("Use --list to see available options")
            return None, None

        if len(matching) > 1:
            print(f"Multiple matches for '{search_term}':")
            for ep in matching:
                print(f"  - {ep.get('local_authority', 'Unknown')}")
            print("Please be more specific")
            return None, None

        endpoint = matching[0]
        la_name = endpoint.get("local_authority", "Unknown")

        return endpoint, la_name
