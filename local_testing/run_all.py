#!/usr/bin/env python3
"""
Script to run the pipeline for all Local Authorities.
"""

import sys
import subprocess
import time
import json
from pathlib import Path
from datetime import datetime
from cli import CLI


def main():
    """Run pipeline for all Local Authorities."""
    # Get limit from command line if provided
    limit = None
    if len(sys.argv) > 1:
        limit = sys.argv[1]

    # Fetch all endpoints
    print("Fetching endpoint list...")
    endpoints = CLI.fetch_endpoint_list()
    print(f"Found {len(endpoints)} Local Authorities")
    print(f"Running with Polars comparison enabled\n")

    success_count = 0
    error_count = 0
    errors = []
    la_times = []
    batch_start = time.time()

    for i, ep in enumerate(endpoints, 1):
        la = ep.get("local_authority", "Unknown")
        print(f"\n{'='*60}")
        print(f"[{i}/{len(endpoints)}] Processing: {la}")
        print(f"{'='*60}")

        # Build command with --compare flag for Polars
        cmd = [sys.executable, "main.py", "--la", la, "--compare"]
        if limit:
            cmd.extend(["--limit", limit])

        # Time this LA
        la_start = time.time()
        result = subprocess.run(cmd)
        la_duration = time.time() - la_start

        if result.returncode != 0:
            print(f"  ⚠️  Error processing {la}")
            error_count += 1
            errors.append(la)
            la_times.append({"la": la, "duration": la_duration, "status": "error"})
        else:
            print(f"  ✅ Completed {la} ({la_duration:.1f}s)")
            success_count += 1
            la_times.append({"la": la, "duration": la_duration, "status": "success"})

    # Calculate batch metrics
    batch_duration = time.time() - batch_start
    avg_duration = (
        sum(t["duration"] for t in la_times) / len(la_times) if la_times else 0
    )
    successful_times = [t["duration"] for t in la_times if t["status"] == "success"]

    # Summary
    print(f"\n{'='*60}")
    print("BATCH PROCESSING COMPLETE (with Polars Comparison)")
    print(f"{'='*60}")
    print(f"  Total LAs:       {len(endpoints)}")
    print(f"  Success:         {success_count}")
    print(f"  Errors:          {error_count}")
    print(f"  Total Time:      {batch_duration:.1f}s ({batch_duration/60:.1f}m)")
    print(f"  Avg Time/LA:     {avg_duration:.1f}s")
    if successful_times:
        print(f"  Min Time:        {min(successful_times):.1f}s")
        print(f"  Max Time:        {max(successful_times):.1f}s")
    print(f"\n  Note: All LAs processed with both Original + Polars pipelines")

    if errors:
        print(f"\nFailed Local Authorities:")
        for la in errors:
            print(f"  - {la}")

    # Save batch report
    reports_dir = Path(__file__).parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    batch_report = {
        "batch_timestamp": timestamp,
        "total_las": len(endpoints),
        "success_count": success_count,
        "error_count": error_count,
        "batch_duration_seconds": batch_duration,
        "average_duration_seconds": avg_duration,
        "polars_comparison_enabled": True,
        "limit": limit,
        "la_results": la_times,
        "errors": errors,
    }

    batch_json = reports_dir / f"batch_{timestamp}_summary.json"
    with open(batch_json, "w") as f:
        json.dump(batch_report, f, indent=2)

    print(f"\nBatch report saved: {batch_json}")
    print(f"{'='*60}\n")

    return 1 if error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
