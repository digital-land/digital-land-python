"""
File downloader for title-boundary GML files.

Handles fetching endpoint lists from GitHub config repository
and downloading ZIP files with progress tracking.
"""

import csv
import urllib.request
from pathlib import Path
from typing import List, Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class FileDownloader:
    """Handles downloading title-boundary files from endpoint CSV."""

    ENDPOINT_CSV_URL = "https://raw.githubusercontent.com/digital-land/config/main/collection/title-boundary/endpoint.csv"

    def __init__(self, endpoint_csv_url: Optional[str] = None):
        """Initialize downloader with optional custom endpoint CSV URL."""
        self.endpoint_csv_url = endpoint_csv_url or self.ENDPOINT_CSV_URL

    def fetch_endpoint_list(self) -> List[dict]:
        """Fetch list of available title boundary datasets from GitHub CSV."""
        print(f"  Fetching endpoint list from {self.endpoint_csv_url}...")

        req = urllib.request.Request(
            self.endpoint_csv_url,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        )
        
        with urllib.request.urlopen(req) as response:
            content = response.read().decode('utf-8')
            reader = csv.DictReader(content.splitlines())
            
            endpoints = []
            for row in reader:
                url = row.get('endpoint-url', '').strip()
                if url:
                    endpoints.append({
                        'endpoint': row.get('endpoint', ''),
                        'url': url,
                        'local_authority': self.get_la_name_from_url(url),
                        'entry_date': row.get('entry-date', ''),
                    })

        print(f"  Found {len(endpoints)} endpoints")
        return endpoints

    @staticmethod
    def get_la_name_from_url(url: str) -> str:
        """Extract Local Authority name from download URL."""
        # URL format: .../download/Buckinghamshire_Council.zip
        parts = url.split("/")
        if parts:
            filename = parts[-1].replace(".zip", "").replace("_", " ")
            # Remove common suffixes for cleaner names
            for suffix in [" Council", " Borough Council", " City Council", " District Council", 
                          " Metropolitan Borough Council", " County Council"]:
                if filename.endswith(suffix):
                    filename = filename[:-len(suffix)]
                    break
            # Remove prefixes
            for prefix in ["Borough of ", "City of ", "County of ", "Royal Borough of ",
                          "London Borough of "]:
                if filename.startswith(prefix):
                    filename = filename[len(prefix):]
                    break
            return filename.strip()
        return "Unknown"

    def download_file(
        self, url: str, output_path: Path, chunk_size: int = 8192
    ) -> Path:
        """
        Download file from URL to output path with progress tracking.

        Args:
            url: URL to download from
            output_path: Path where file should be saved
            chunk_size: Size of download chunks in bytes

        Returns:
            Path to downloaded file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"  Downloading from {url}")
        print(f"  Output: {output_path}")

        # Use requests library if available (better redirect/cookie handling)
        if HAS_REQUESTS:
            return self._download_with_requests(url, output_path, chunk_size)
        else:
            return self._download_with_urllib(url, output_path, chunk_size)
    
    def _download_with_requests(self, url: str, output_path: Path, chunk_size: int) -> Path:
        """Download using requests library (handles redirects better)."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, stream=True, allow_redirects=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        mb_downloaded = downloaded / (1024 * 1024)
                        mb_total = total_size / (1024 * 1024)
                        print(f"\r  Progress: {progress:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end="", flush=True)
        
        print()  # New line after progress
        print(f"  ✓ Downloaded {downloaded:,} bytes")
        return output_path
    
    def _download_with_urllib(self, url: str, output_path: Path, chunk_size: int) -> Path:
        """Download using urllib (fallback)."""

        # Add comprehensive browser headers to mimic real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(output_path, "wb") as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)

                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(
                            f"\r  Progress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)",
                            end="",
                        )

        print()  # New line after progress
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {size_mb:.1f} MB")

        return output_path
