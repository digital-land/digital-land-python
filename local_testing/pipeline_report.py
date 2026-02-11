"""
Performance reporting and metrics tracking for pipeline runs.

Provides classes to track timing, resource usage, and comparison
metrics for original vs Polars pipeline implementations.
"""

import sys
import time
import platform as plat
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class PhaseMetrics:
    """Metrics for a single pipeline phase."""

    name: str
    phase_number: int
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    input_count: int = 0
    output_count: int = 0

    def complete(self, output_count: int = 0):
        """Mark phase as complete and calculate duration."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.output_count = output_count


@dataclass
class StepMetrics:
    """Metrics for a pipeline step (Download, Extract, Convert, Transform)."""

    name: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    success: bool = True
    details: Dict[str, Any] = field(default_factory=dict)

    def start(self):
        """Start timing this step."""
        self.start_time = time.time()

    def complete(self, **details):
        """Mark step as complete."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.details.update(details)

    def mark_complete(self, success: bool = True, **details):
        """Mark step as complete with success status."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.success = success
        self.details.update(details)


@dataclass
class PipelineReport:
    """Complete performance report for a pipeline run."""

    # Run metadata
    run_id: str = ""
    timestamp: str = ""
    local_authority: str = ""
    dataset: str = "title-boundary"
    record_limit: Optional[int] = None

    # Input/Output metrics
    input_records: int = 0
    harmonised_records: int = 0
    fact_records: int = 0

    # Polars comparison metrics
    polars_harmonised_records: int = 0
    polars_fact_records: int = 0
    polars_phases: List[PhaseMetrics] = field(default_factory=list)
    polars_transform_seconds: float = 0.0

    # File sizes
    zip_size_mb: float = 0.0
    gml_size_mb: float = 0.0
    csv_size_mb: float = 0.0

    # Step timings
    steps: Dict[str, StepMetrics] = field(default_factory=dict)

    # Phase timings (transformation only)
    phases: List[PhaseMetrics] = field(default_factory=list)

    # Phase selection (if running specific phases)
    selected_phases: Optional[set] = None

    # Total timing
    total_duration_seconds: float = 0.0
    transform_duration_seconds: float = 0.0

    # System info
    python_version: str = ""
    platform: str = ""

    def __post_init__(self):
        """Initialize run metadata."""
        self.python_version = sys.version.split()[0]
        self.platform = f"{plat.system()} {plat.release()}"
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.timestamp = datetime.now().isoformat()

    def add_step(self, name: str) -> StepMetrics:
        """Add and start a new step."""
        step = StepMetrics(name=name)
        step.start()
        self.steps[name] = step
        return step

    def add_phase(self, name: str, phase_number: int) -> PhaseMetrics:
        """Add a new phase."""
        phase = PhaseMetrics(
            name=name, phase_number=phase_number, start_time=time.time()
        )
        self.phases.append(phase)
        return phase

    def calculate_totals(self):
        """Calculate total durations."""
        self.total_duration_seconds = sum(
            s.duration_seconds for s in self.steps.values()
        )
        self.transform_duration_seconds = sum(p.duration_seconds for p in self.phases)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        # Filter phases if selection is active
        phases_to_output = self.phases
        polars_phases_to_output = self.polars_phases
        if self.selected_phases:
            phases_to_output = [
                p for p in self.phases if p.phase_number in self.selected_phases
            ]
            polars_phases_to_output = [
                p for p in self.polars_phases if p.phase_number in self.selected_phases
            ]

        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "local_authority": self.local_authority,
            "dataset": self.dataset,
            "record_limit": self.record_limit,
            "selected_phases": (
                list(sorted(self.selected_phases)) if self.selected_phases else None
            ),
            "input_records": self.input_records,
            "harmonised_records": self.harmonised_records,
            "fact_records": self.fact_records,
            "file_sizes": {
                "zip_mb": self.zip_size_mb,
                "gml_mb": self.gml_size_mb,
                "csv_mb": self.csv_size_mb,
            },
            "timing": {
                "total_seconds": self.total_duration_seconds,
                "transform_seconds": self.transform_duration_seconds,
                "polars_transform_seconds": self.polars_transform_seconds,
                "speedup_factor": (
                    (self.transform_duration_seconds / self.polars_transform_seconds)
                    if self.polars_transform_seconds > 0
                    else 0
                ),
                "steps": {
                    name: {"duration_seconds": s.duration_seconds, **s.details}
                    for name, s in self.steps.items()
                },
                "phases": [
                    {
                        "number": p.phase_number,
                        "name": p.name,
                        "duration_seconds": p.duration_seconds,
                        "output_count": p.output_count,
                    }
                    for p in phases_to_output
                ],
                "polars_phases": [
                    {
                        "number": p.phase_number,
                        "name": p.name,
                        "duration_seconds": p.duration_seconds,
                        "output_count": p.output_count,
                    }
                    for p in polars_phases_to_output
                ],
            },
            "comparison": {
                "original_transform_seconds": self.transform_duration_seconds,
                "polars_transform_seconds": self.polars_transform_seconds,
                "speedup_factor": (
                    (self.transform_duration_seconds / self.polars_transform_seconds)
                    if self.polars_transform_seconds > 0
                    else 0
                ),
                "time_saved_seconds": self.transform_duration_seconds
                - self.polars_transform_seconds,
            },
            "system": {
                "python_version": self.python_version,
                "platform": self.platform,
            },
        }

    def generate_text_report(self) -> str:
        """Generate human-readable text report."""
        lines = []
        lines.append("=" * 100)
        lines.append("TITLE BOUNDARY PIPELINE - PERFORMANCE REPORT")
        lines.append("=" * 100)
        lines.append("")
        lines.append(f"Run ID:          {self.run_id}")
        lines.append(f"Timestamp:       {self.timestamp}")
        lines.append(f"Local Authority: {self.local_authority}")
        lines.append(f"Dataset:         {self.dataset}")
        lines.append(f"Record Limit:    {self.record_limit or 'None (all records)'}")
        lines.append("")

        lines.append("-" * 100)
        lines.append("INPUT/OUTPUT SUMMARY")
        lines.append("-" * 100)
        lines.append(f"Input Records:      {self.input_records:,}")
        if self.polars_phases:
            lines.append(
                f"Harmonised Records: {self.harmonised_records:,} (Original) / {self.polars_harmonised_records:,} (Polars)"
            )
            lines.append(
                f"Fact Records:       {self.fact_records:,} (Original) / {self.polars_fact_records:,} (Polars)"
            )
        else:
            lines.append(f"Harmonised Records: {self.harmonised_records:,}")
            lines.append(f"Fact Records:       {self.fact_records:,}")
        lines.append("")

        lines.append("-" * 100)
        lines.append("FILE SIZES")
        lines.append("-" * 100)
        lines.append(f"ZIP File:  {self.zip_size_mb:,.2f} MB")
        lines.append(f"GML File:  {self.gml_size_mb:,.2f} MB")
        lines.append(f"CSV File:  {self.csv_size_mb:,.2f} MB")
        lines.append("")

        lines.append("-" * 100)
        lines.append("STEP TIMING SUMMARY")
        lines.append("-" * 100)
        lines.append(f"{'Step':<20} {'Duration':>12} {'% of Total':>12}")
        lines.append("-" * 44)
        for name, step in self.steps.items():
            pct = (
                (step.duration_seconds / self.total_duration_seconds * 100)
                if self.total_duration_seconds > 0
                else 0
            )
            lines.append(f"{name:<20} {step.duration_seconds:>10.3f}s {pct:>10.1f}%")
        lines.append("-" * 44)
        lines.append(
            f"{'TOTAL':<20} {self.total_duration_seconds:>10.3f}s {100.0:>10.1f}%"
        )
        lines.append("")

        # COMBINED PHASE COMPARISON TABLE (if Polars was run)
        if self.polars_phases:
            lines.append("=" * 100)
            lines.append("PHASE-BY-PHASE COMPARISON: ORIGINAL vs POLARS")
            lines.append("=" * 100)

            # Show phase selection info if applicable
            if self.selected_phases:
                lines.append(f"Running selected phases: {sorted(self.selected_phases)}")
            lines.append("")

            # Header
            header = (
                f"{'#':<3} {'Phase Name':<26} {'Original':>11} {'Polars':>11} "
                f"{'Speedup':>10} {'Time Saved':>12} {'Orig Out':>10} {'Polars Out':>10}"
            )
            lines.append(header)
            lines.append("-" * 100)

            # Build lookup for Polars phases by phase number (not name, since names may differ)
            polars_by_number = {p.phase_number: p for p in self.polars_phases}

            # Filter phases if selection is active
            phases_to_display = self.phases
            if self.selected_phases:
                phases_to_display = [
                    p for p in self.phases if p.phase_number in self.selected_phases
                ]

            total_original = 0.0
            total_polars = 0.0
            total_saved = 0.0

            for phase in phases_to_display:
                polars_phase = polars_by_number.get(phase.phase_number)
                if polars_phase:
                    if polars_phase.duration_seconds > 0:
                        speedup = phase.duration_seconds / polars_phase.duration_seconds
                    else:
                        speedup = float("inf") if phase.duration_seconds > 0 else 1.0

                    saved = phase.duration_seconds - polars_phase.duration_seconds
                    speedup_str = f"{speedup:.1f}x" if speedup != float("inf") else "∞"

                    # Show phase name(s) - if different, show "Original→Polars"
                    if phase.name != polars_phase.name:
                        phase_display = f"{phase.name}→{polars_phase.name}"
                    else:
                        phase_display = phase.name
                    
                    # Truncate if too long
                    if len(phase_display) > 26:
                        phase_display = phase_display[:23] + "..."

                    phase_line = (
                        f"{phase.phase_number:<3} {phase_display:<26} "
                        f"{phase.duration_seconds:>9.4f}s {polars_phase.duration_seconds:>9.4f}s "
                        f"{speedup_str:>9} {saved:>10.4f}s {phase.output_count:>10,} "
                        f"{polars_phase.output_count:>10,}"
                    )
                    lines.append(phase_line)

                    total_original += phase.duration_seconds
                    total_polars += polars_phase.duration_seconds
                    total_saved += saved
                else:
                    lines.append(
                        f"{phase.phase_number:<3} {phase.name:<26} {phase.duration_seconds:>9.4f}s {'N/A':>11} {'N/A':>9} {'N/A':>12} {phase.output_count:>10,} {'N/A':>10}"
                    )

            lines.append("-" * 100)
            overall_speedup = total_original / total_polars if total_polars > 0 else 0
            lines.append(
                f"{'':3} {'TOTAL TRANSFORM TIME':<26} {total_original:>9.4f}s {total_polars:>9.4f}s {overall_speedup:>8.1f}x {total_saved:>10.4f}s"
            )
            lines.append("")

            # Overall summary
            lines.append("-" * 100)
            lines.append("PERFORMANCE SUMMARY")
            lines.append("-" * 100)
            lines.append(f"Original Pipeline:  {total_original:.4f}s")
            lines.append(f"Polars Pipeline:    {total_polars:.4f}s")
            lines.append(f"Speedup Factor:     {overall_speedup:.1f}x faster")
            lines.append(
                f"Time Saved:         {total_saved:.4f}s ({(total_saved/total_original*100):.1f}% reduction)"
            )
            lines.append("")

        else:
            lines.append("-" * 100)
            lines.append("ORIGINAL PIPELINE - PHASE TIMING (Row-by-Row)")
            lines.append("-" * 100)

            # Show phase selection info if applicable
            if self.selected_phases:
                lines.append(f"Running selected phases: {sorted(self.selected_phases)}")
                lines.append("")

            lines.append(
                f"{'#':<4} {'Phase Name':<30} {'Duration':>12} {'% of Transform':>14} {'Output':>10}"
            )
            lines.append("-" * 74)

            # Filter phases if selection is active
            phases_to_display = self.phases
            if self.selected_phases:
                phases_to_display = [
                    p for p in self.phases if p.phase_number in self.selected_phases
                ]

            for phase in phases_to_display:
                pct = (
                    (phase.duration_seconds / self.transform_duration_seconds * 100)
                    if self.transform_duration_seconds > 0
                    else 0
                )
                lines.append(
                    f"{phase.phase_number:<4} {phase.name:<30} {phase.duration_seconds:>10.4f}s {pct:>12.1f}% {phase.output_count:>10,}"
                )

            lines.append("-" * 74)
            lines.append(
                f"{'':4} {'TOTAL TRANSFORM TIME':<30} {self.transform_duration_seconds:>10.4f}s {100.0:>12.1f}%"
            )
            lines.append("")

        # Top 5 slowest phases (Original)
        lines.append("-" * 100)
        lines.append("TOP 5 SLOWEST PHASES (Original Pipeline)")
        lines.append("-" * 100)

        # Filter phases for "top slowest" if selection is active
        phases_for_top5 = self.phases
        if self.selected_phases:
            phases_for_top5 = [
                p for p in self.phases if p.phase_number in self.selected_phases
            ]

        sorted_phases = sorted(
            phases_for_top5, key=lambda x: x.duration_seconds, reverse=True
        )[:5]
        for i, phase in enumerate(sorted_phases, 1):
            pct = (
                (phase.duration_seconds / self.transform_duration_seconds * 100)
                if self.transform_duration_seconds > 0
                else 0
            )
            lines.append(
                f"  {i}. {phase.name:<30} {phase.duration_seconds:>10.4f}s ({pct:.1f}%)"
            )
        lines.append("")

        # TOP SPEEDUP WINNERS (if Polars was run)
        if self.polars_phases:
            lines.append("-" * 100)
            lines.append("TOP 5 SPEEDUP WINNERS (Biggest Improvements with Polars)")
            lines.append("-" * 100)

            # Filter phases for speedup calculation if selection is active
            phases_for_speedup = self.phases
            if self.selected_phases:
                phases_for_speedup = [
                    p for p in self.phases if p.phase_number in self.selected_phases
                ]

            polars_by_name = {p.name: p for p in self.polars_phases}
            speedups = []
            for phase in phases_for_speedup:
                polars_phase = polars_by_name.get(phase.name)
                if polars_phase and phase.duration_seconds > 0.0001:
                    if polars_phase.duration_seconds > 0:
                        speedup = phase.duration_seconds / polars_phase.duration_seconds
                    else:
                        speedup = float("inf")
                    saved = phase.duration_seconds - polars_phase.duration_seconds
                    speedups.append(
                        (
                            phase.name,
                            phase.duration_seconds,
                            polars_phase.duration_seconds,
                            speedup,
                            saved,
                        )
                    )

            speedups.sort(key=lambda x: x[4], reverse=True)

            for i, (name, orig, polars, spd, saved) in enumerate(speedups[:5], 1):
                spd_str = f"{spd:.1f}x" if spd != float("inf") else "∞"
                lines.append(
                    f"  {i}. {name:<26} {orig:.4f}s → {polars:.4f}s  ({spd_str} faster, {saved:.4f}s saved)"
                )
            lines.append("")

        # THROUGHPUT METRICS
        if (
            self.polars_phases
            and self.input_records > 0
            and self.transform_duration_seconds > 0
            and self.polars_transform_seconds > 0
        ):
            lines.append("-" * 100)
            lines.append("THROUGHPUT METRICS")
            lines.append("-" * 100)
            orig_throughput = self.input_records / self.transform_duration_seconds
            polars_throughput = self.input_records / self.polars_transform_seconds
            lines.append(f"Original Pipeline:  {orig_throughput:,.0f} records/second")
            lines.append(f"Polars Pipeline:    {polars_throughput:,.0f} records/second")
            lines.append(
                f"Throughput Gain:    {polars_throughput - orig_throughput:,.0f} records/second faster"
            )
            lines.append("")

        lines.append("=" * 100)
        lines.append(
            f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        lines.append("=" * 100)

        return "\n".join(lines)

    def save_json(self, path: Path):
        """Save report as JSON file."""
        import json

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    def save_text(self, path: Path):
        """Save report as text file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.generate_text_report())
