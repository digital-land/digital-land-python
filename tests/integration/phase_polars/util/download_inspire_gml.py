#!/usr/bin/env python3
"""
Download all INSPIRE Index Polygon GML files from:
    https://use-land-property-data.service.gov.uk/datasets/inspire/download

Each council entry links to a ZIP archive which contains a single .gml file.
The GML files are extracted to:
    tests/integration/data/gml/

Usage
-----
    python tests/integration/phase_polars/util/download_inspire_gml.py [OPTIONS]

Options
-------
    --output-dir PATH     Override the default output directory.
    --workers INT         Number of parallel download threads (default: 4).
    --skip-existing       Skip councils whose GML file is already present (default: True).
    --council NAME        Download only a specific council by name (substring match).
    --dry-run             Print the download URLs without downloading anything.
"""

import argparse
import io
import logging
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
BASE_URL = "https://use-land-property-data.service.gov.uk"
DOWNLOAD_PAGE = f"{BASE_URL}/datasets/inspire/download"

DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parents[2] / "data" / "gml"  # tests/integration/
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------


def _get_download_links(session: requests.Session) -> list[tuple[str, str]]:
    """Return a list of (council_name, absolute_url) pairs from the download page.

    Each entry whose anchor text is "Download .gml" (case-insensitive) is included.
    """
    log.info("Fetching download page: %s", DOWNLOAD_PAGE)
    response = session.get(DOWNLOAD_PAGE, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    links: list[tuple[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(strip=True).lower()
        if "download" in text and "gml" in text:
            href = anchor["href"]
            full_url = urljoin(BASE_URL, href)

            # Try to get the council name from the nearest table row sibling cell.
            council_name: str | None = None
            td = anchor.find_parent("td")
            if td:
                prev_td = td.find_previous_sibling("td")
                if prev_td:
                    council_name = prev_td.get_text(strip=True) or None
            if not council_name:
                tr = anchor.find_parent("tr")
                if tr:
                    first_td = tr.find("td")
                    if first_td and first_td is not td:
                        council_name = first_td.get_text(strip=True) or None

            # Fall back to deriving the name from the URL path stem.
            if not council_name:
                stem = Path(href.rstrip("/").split("/")[-1]).stem
                council_name = stem.replace("_", " ")

            links.append((council_name, full_url))

    if not links:
        log.warning(
            "No 'Download .gml' links found on the page. "
            "The page structure may have changed."
        )
    else:
        log.info("Found %d council download links.", len(links))

    return links


# ---------------------------------------------------------------------------
# Download + extract
# ---------------------------------------------------------------------------


def _safe_filename(council_name: str) -> str:
    """Convert a council name to a safe filesystem-friendly filename stem."""
    return "".join(
        c if c.isalnum() or c in " -_()" else "_" for c in council_name
    ).strip()


def _download_one(
    session: requests.Session,
    council_name: str,
    url: str,
    output_dir: Path,
    skip_existing: bool,
) -> tuple[str, str]:
    """Download and extract GML for one council.

    Returns (council_name, status) where status is one of:
        'skipped', 'ok', or an error message.
    """
    safe_stem = _safe_filename(council_name)
    # Check whether a GML file for this council already exists.
    existing = list(output_dir.glob(f"{safe_stem}*.gml"))
    if skip_existing and existing:
        return council_name, "skipped"

    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")

        # The response may be a ZIP archive or a raw GML file.
        # Always save under safe_stem so --skip-existing works and multiple
        # councils can coexist (every INSPIRE ZIP contains the same generic
        # filename "Land_Registry_Cadastral_Parcels.gml").
        dest_name = f"{safe_stem}.gml"
        if "zip" in content_type or url.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for member in zf.namelist():
                    if member.lower().endswith(".gml"):
                        (output_dir / dest_name).write_bytes(zf.read(member))
                        break
        elif (
            "gml" in content_type
            or "xml" in content_type
            or url.lower().endswith(".gml")
        ):
            (output_dir / dest_name).write_bytes(resp.content)
        else:
            # Attempt ZIP extraction as a fallback for unknown content types.
            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    for member in zf.namelist():
                        if member.lower().endswith(".gml"):
                            (output_dir / dest_name).write_bytes(zf.read(member))
                            break
            except zipfile.BadZipFile:
                # Last resort: save as .gml directly.
                (output_dir / dest_name).write_bytes(resp.content)

        return council_name, "ok"

    except Exception as exc:  # noqa: BLE001
        return council_name, f"ERROR: {exc}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all INSPIRE GML files from the HM Land Registry service."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write GML files into (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel download threads (default: 4)",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip councils whose GML file already exists (default: enabled)",
    )
    parser.add_argument(
        "--council",
        type=str,
        default=None,
        help="Download only councils whose name contains this substring (case-insensitive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print download URLs without downloading anything",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Output directory: %s", output_dir)

    session = requests.Session()
    session.headers["User-Agent"] = (
        "digital-land-python/inspire-downloader "
        "(https://github.com/digital-land/digital-land-python)"
    )

    try:
        links = _get_download_links(session)
    except requests.HTTPError as exc:
        log.error("Failed to fetch download page: %s", exc)
        return 1

    if not links:
        return 1

    # Apply optional council filter.
    if args.council:
        filter_lower = args.council.lower()
        links = [(name, url) for name, url in links if filter_lower in name.lower()]
        log.info("Filtered to %d councils matching %r.", len(links), args.council)

    if args.dry_run:
        for council, url in links:
            print(f"{council}\t{url}")
        return 0

    # Download with a thread pool.
    ok = skipped = errors = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                _download_one, session, council, url, output_dir, args.skip_existing
            ): council
            for council, url in links
        }
        total = len(futures)
        done = 0
        for future in as_completed(futures):
            council_name, status = future.result()
            done += 1
            if status == "ok":
                ok += 1
                log.info("[%d/%d] ✓ %s", done, total, council_name)
            elif status == "skipped":
                skipped += 1
                log.debug("[%d/%d] — skipped %s", done, total, council_name)
            else:
                errors += 1
                log.error("[%d/%d] %s  (%s)", done, total, council_name, status)

    log.info(
        "Done. downloaded=%d  skipped=%d  errors=%d  total=%d",
        ok,
        skipped,
        errors,
        total,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
