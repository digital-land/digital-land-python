#!/usr/bin/env python3
"""
Title Boundary Pipeline - Download, Convert, and Transform GML data from Land Registry

Orchestration script that coordinates multiple specialized classes.
"""

import sys
import time
from pathlib import Path
from datetime import datetime

from cli import CLI
from file_downloader import FileDownloader
from gml_extractor import GMLExtractor
from gml_converter import GMLConverter
from pipeline_config import PipelineConfig
from pipeline_runner import PipelineRunner
from pipeline_report import PipelineReport


# =============================================================================
# Constants
# =============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
DATASET = "title-boundary"


# =============================================================================
# Helper Functions
# =============================================================================


def parse_phase_selection(phases_str: str) -> set:
    """
    Parse phase selection string into set of phase numbers.

    Args:
        phases_str: Comma-separated phase numbers or ranges (e.g., "1,2,9" or "1-5,9")

    Returns:
        Set of selected phase numbers, or None if invalid
    """
    phases = set()
    try:
        for part in phases_str.split(","):
            part = part.strip()
            if "-" in part:
                # Range: "1-5"
                start, end = part.split("-")
                phases.update(range(int(start), int(end) + 1))
            else:
                # Single phase: "9"
                phases.add(int(part))

        # Validate phase numbers (1-26)
        if any(p < 1 or p > 26 for p in phases):
            return None

        return phases
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    """Main entry point for title-boundary pipeline."""

    # Parse arguments using CLI class
    parser = CLI.create_parser()
    args = parser.parse_args()

    # Setup directories
    raw_dir = SCRIPT_DIR / "raw"
    extracted_dir = SCRIPT_DIR / "extracted"
    converted_dir = SCRIPT_DIR / "converted"
    output_dir = SCRIPT_DIR / "output"
    pipeline_dir = SCRIPT_DIR / "pipeline"
    specification_dir = SCRIPT_DIR.parent / "specification"
    cache_dir = SCRIPT_DIR / "cache"
    reports_dir = SCRIPT_DIR / "reports"

    for directory in [
        raw_dir,
        extracted_dir,
        converted_dir,
        output_dir,
        pipeline_dir,
        cache_dir,
        reports_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    # List mode - use CLI class
    if args.list or not args.la:
        CLI.list_available_las()
        if not args.la:
            print("Use --la 'Name' to process a specific Local Authority")
        return 0

    # Find matching LA - use CLI class
    endpoint, la_name = CLI.find_matching_la(args.la)
    if not endpoint:
        return 1

    # Initialize
    la_slug = la_name.lower().replace(" ", "_").replace(",", "")
    report = PipelineReport()
    report.local_authority = la_name
    report.dataset = DATASET

    # Print header
    print(f"\n{'='*60}")
    print("Title Boundary Pipeline")
    print(f"{'='*60}")
    print(f"Local Authority: {la_name}")
    print(f"Endpoint: {endpoint['url']}")
    if args.limit:
        print(f"Limit: {args.limit:,} records")
    print(f"{'='*60}\n")

    overall_start = time.time()

    # =========================================================================
    # Step 1: Download - use FileDownloader class
    # =========================================================================
    print("Step 1: Download")
    print("-" * 40)

    step_download = report.add_step("Download")
    zip_path = raw_dir / f"{la_slug}.zip"

    if args.skip_download and zip_path.exists():
        print(f"  Using existing: {zip_path}")
        step_download.mark_complete(success=True)
    else:
        downloader = FileDownloader()
        success = downloader.download_file(endpoint["url"], zip_path)
        step_download.mark_complete(success=success)
        if not success:
            print("  Download failed")
            return 1

    if zip_path.exists():
        report.zip_size_mb = zip_path.stat().st_size / (1024 * 1024)

    # =========================================================================
    # Step 2: Extract - use GMLExtractor class
    # =========================================================================
    print("\nStep 2: Extract")
    print("-" * 40)

    step_extract = report.add_step("Extract")
    extract_subdir = extracted_dir / la_slug

    try:
        gml_path = GMLExtractor.extract_gml_from_zip(zip_path, extract_subdir)
        step_extract.mark_complete(success=True)

        if gml_path.exists():
            report.gml_size_mb = gml_path.stat().st_size / (1024 * 1024)
    except Exception as e:
        print(f"  Extraction failed: {e}")
        step_extract.mark_complete(success=False)
        return 1

    # =========================================================================
    # Step 3: Convert - use GMLConverter class
    # =========================================================================
    print(f"\nStep 3: Convert GML to Parquet")
    print("-" * 40)

    step_convert = report.add_step("Convert")
    converter = GMLConverter()

    # Choose conversion method based on arguments
    # Always output Parquet for optimal Polars pipeline performance
    if args.use_duckdb:
        method = "DuckDB+Parquet"
        output_path = converted_dir / f"{la_slug}.parquet"
        record_count = converter.convert_to_parquet_duckdb(
            gml_path, output_path, limit=args.limit
        )
    else:
        method = "Polars+Parquet"
        output_path = converted_dir / f"{la_slug}.parquet"
        record_count = converter.convert_to_parquet(
            gml_path, output_path, limit=args.limit
        )

    step_convert.mark_complete(
        success=record_count > 0, record_count=record_count, method=method
    )

    if record_count == 0:
        print("  Conversion produced no records")
        return 1

    report.input_records = record_count

    # =========================================================================
    # Step 4: Transform - use PipelineConfig and PipelineRunner classes
    # =========================================================================
    print("\nStep 4: Transform through Pipeline")
    print("-" * 40)

    step_transform = report.add_step("Transform")

    # Ensure configuration exists using PipelineConfig class
    PipelineConfig.ensure_pipeline_config(pipeline_dir)

    if not specification_dir.exists():
        print(f"  Error: Specification directory not found: {specification_dir}")
        print(f"  Please clone specification to: {specification_dir}")
        step_transform.mark_complete(success=False)
        return 1

    # Parse phase selection if provided
    selected_phases = None
    if args.phases:
        selected_phases = parse_phase_selection(args.phases)
        if selected_phases:
            print(f"  Running selected phases: {sorted(selected_phases)}")
            report.selected_phases = selected_phases  # Store in report for filtering
        else:
            print(f"  Invalid phase selection: {args.phases}")
            step_transform.mark_complete(success=False)
            return 1

    # Run pipeline using PipelineRunner class
    runner = PipelineRunner(dataset=DATASET)
    results = runner.run_full_pipeline(
        input_csv=output_path,
        output_dir=output_dir,
        specification_dir=specification_dir,
        pipeline_dir=pipeline_dir,
        cache_dir=cache_dir,
        la_name=la_name,
        report=report,
        selected_phases=selected_phases,
    )

    step_transform.mark_complete(
        success=True,
        harmonised_records=results["harmonised"],
        fact_records=results["facts"],
        transform_time=results.get("transform_time", 0),
    )

    # Run Polars pipeline for comparison if requested
    if args.compare:
        print("\n  Running Polars pipeline for comparison...")
        from polars_phases import run_polars_pipeline
        import polars as pl

        # Polars pipeline requires Parquet - convert CSV if needed
        polars_input = output_path
        if output_path.suffix.lower() == ".csv":
            polars_input = output_path.with_suffix(".parquet")
            if not polars_input.exists():
                print(f"  Converting CSV to Parquet for Polars pipeline...")
                pl.read_csv(output_path).write_parquet(polars_input)

        # Define required parameters
        field_datatype_map = {"geometry": "text"}  # Simplified for now
        intermediate_fieldnames = ["entity", "name", "geometry", "organisation"]
        factor_fieldnames = ["entity", "fact"]

        polars_harmonised = output_dir / f"{la_name}_polars_harmonised.csv"
        polars_facts = output_dir / f"{la_name}_polars_facts.csv"

        polars_start = time.time()
        polars_metrics, polars_harm_count, polars_fact_count = run_polars_pipeline(
            input_csv=polars_input,
            harmonised_csv=polars_harmonised,
            facts_csv=polars_facts,
            field_datatype_map=field_datatype_map,
            intermediate_fieldnames=intermediate_fieldnames,
            factor_fieldnames=factor_fieldnames,
            dataset=DATASET,
            selected_phases=selected_phases,  # Pass phase selection to Polars
        )
        polars_end = time.time()

        # Store Polars metrics in report
        report.polars_phases = []
        for metric in polars_metrics:
            from pipeline_report import PhaseMetrics

            phase_metric = PhaseMetrics(
                name=metric.name,
                phase_number=metric.phase_number,
                start_time=0,
                end_time=0,
                duration_seconds=metric.duration_seconds,
                input_count=metric.input_count,
                output_count=metric.output_count,
            )
            report.polars_phases.append(phase_metric)

        report.polars_harmonised_records = polars_harm_count
        report.polars_fact_records = polars_fact_count
        report.polars_transform_seconds = polars_end - polars_start

        speedup = (
            results.get("transform_time", 0) / report.polars_transform_seconds
            if report.polars_transform_seconds > 0
            else 0
        )
        print(f"  Polars transform time: {report.polars_transform_seconds:.3f}s")
        print(f"  Speedup: {speedup:.1f}x faster")

    # =========================================================================
    # Step 5: Generate Report - use PipelineReport class
    # =========================================================================
    overall_end = time.time()
    report.total_duration_seconds = overall_end - overall_start
    report.calculate_totals()

    print("\nStep 5: Generate Performance Report")
    print("-" * 40)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"{la_slug}_{timestamp}_performance.json"
    text_path = reports_dir / f"{la_slug}_{timestamp}_performance.txt"

    report.save_json(json_path)
    report.save_text(text_path)

    print(f"  JSON report: {json_path}")
    print(f"  Text report: {text_path}")

    # =========================================================================
    # Summary
    # =========================================================================
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Local Authority: {la_name}")
    print(f"Dataset: {DATASET}")
    print(f"Total Duration: {report.total_duration_seconds:.2f}s")
    print(f"Input Records: {report.input_records:,}")
    print(f"Harmonised Records: {report.harmonised_records:,}")
    print(f"Fact Records: {report.fact_records:,}")

    if report.steps:
        print("\nStep Summary:")
        for name, step in report.steps.items():
            status = "✓" if step.success else "✗"
            print(f"  {status} {name:<20} {step.duration_seconds:8.3f}s")

    if report.phases:
        total_phase_time = sum(p.duration_seconds for p in report.phases)
        print(
            f"\nTransform Phases: {len(report.phases)} phases, {total_phase_time:.3f}s total"
        )

    print(f"{'='*60}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
