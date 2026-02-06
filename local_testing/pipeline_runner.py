"""
Pipeline execution engine for title-boundary dataset.

Handles running the full 26-phase digital-land transformation pipeline
with detailed timing and progress tracking.
"""

import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

from pipeline_config import PipelineConfig


class PipelineRunner:
    """Executes the digital-land transformation pipeline with timing."""

    def __init__(self, dataset: str = "title-boundary"):
        """
        Initialize pipeline runner.

        Args:
            dataset: Name of the dataset being processed
        """
        self.dataset = dataset
        self.pipeline_imports = None

    def get_pipeline_imports(self):
        """
        Lazy import of digital-land pipeline modules.

        Returns dict of imported classes and functions.
        """
        if self.pipeline_imports is not None:
            return self.pipeline_imports

        from digital_land.phase.convert import ConvertPhase
        from digital_land.phase.normalise import NormalisePhase
        from digital_land.phase.parse import ParsePhase
        from digital_land.phase.concat import ConcatFieldPhase
        from digital_land.phase.filter import FilterPhase
        from digital_land.phase.map import MapPhase
        from digital_land.phase.patch import PatchPhase
        from digital_land.phase.harmonise import HarmonisePhase
        from digital_land.phase.default import DefaultPhase
        from digital_land.phase.migrate import MigratePhase
        from digital_land.phase.organisation import OrganisationPhase
        from digital_land.phase.prune import (
            FieldPrunePhase,
            EntityPrunePhase,
            FactPrunePhase,
        )
        from digital_land.phase.reference import (
            EntityReferencePhase,
            FactReferencePhase,
        )
        from digital_land.phase.prefix import EntityPrefixPhase
        from digital_land.phase.lookup import EntityLookupPhase, FactLookupPhase
        from digital_land.phase.priority import PriorityPhase
        from digital_land.phase.pivot import PivotPhase
        from digital_land.phase.combine import FactCombinePhase
        from digital_land.phase.factor import FactorPhase
        from digital_land.phase.save import SavePhase
        from digital_land.pipeline.main import Pipeline
        from digital_land.specification import Specification
        from digital_land.organisation import Organisation
        from digital_land.log import (
            IssueLog,
            ColumnFieldLog,
            DatasetResourceLog,
            OperationalIssueLog,
            ConvertedResourceLog,
        )
        from digital_land.api import API

        self.pipeline_imports = {
            "ConvertPhase": ConvertPhase,
            "NormalisePhase": NormalisePhase,
            "ParsePhase": ParsePhase,
            "ConcatFieldPhase": ConcatFieldPhase,
            "FilterPhase": FilterPhase,
            "MapPhase": MapPhase,
            "PatchPhase": PatchPhase,
            "HarmonisePhase": HarmonisePhase,
            "DefaultPhase": DefaultPhase,
            "MigratePhase": MigratePhase,
            "OrganisationPhase": OrganisationPhase,
            "FieldPrunePhase": FieldPrunePhase,
            "EntityPrunePhase": EntityPrunePhase,
            "FactPrunePhase": FactPrunePhase,
            "EntityReferencePhase": EntityReferencePhase,
            "FactReferencePhase": FactReferencePhase,
            "EntityPrefixPhase": EntityPrefixPhase,
            "EntityLookupPhase": EntityLookupPhase,
            "FactLookupPhase": FactLookupPhase,
            "PriorityPhase": PriorityPhase,
            "PivotPhase": PivotPhase,
            "FactCombinePhase": FactCombinePhase,
            "FactorPhase": FactorPhase,
            "SavePhase": SavePhase,
            "Pipeline": Pipeline,
            "Specification": Specification,
            "Organisation": Organisation,
            "IssueLog": IssueLog,
            "ColumnFieldLog": ColumnFieldLog,
            "DatasetResourceLog": DatasetResourceLog,
            "OperationalIssueLog": OperationalIssueLog,
            "ConvertedResourceLog": ConvertedResourceLog,
            "API": API,
        }

        return self.pipeline_imports

    def run_full_pipeline(
        self,
        input_csv: Path,
        output_dir: Path,
        specification_dir: Path,
        pipeline_dir: Path,
        cache_dir: Path,
        la_name: str,
        report=None,
        selected_phases=None,
    ) -> Dict:
        """
        Run the full 26-phase digital-land transformation pipeline.

        Args:
            input_csv: Path to input CSV/Parquet file
            output_dir: Directory for output files
            specification_dir: Directory containing specification files
            pipeline_dir: Directory containing pipeline configuration
            cache_dir: Directory for cached resources
            la_name: Local Authority name/slug
            report: Optional PipelineReport instance for metrics tracking
            selected_phases: Optional set of phase numbers (1-26) to run

        Returns:
            Dict with results including file paths and record counts
        """
        print("  Loading digital-land pipeline modules...")
        p = self.get_pipeline_imports()

        # Convert Parquet to CSV if needed (original pipeline only supports CSV)
        if input_csv.suffix.lower() == ".parquet":
            import polars as pl

            csv_input = input_csv.with_suffix(".csv")
            if not csv_input.exists():
                print(f"  Converting Parquet to CSV for original pipeline...")
                pl.read_parquet(input_csv).write_csv(csv_input)
            input_csv = csv_input

        # Set up output paths
        harmonised_csv = output_dir / f"{la_name}_harmonised.csv"
        facts_csv = output_dir / f"{la_name}_facts.csv"
        issue_csv = output_dir / f"{la_name}_issues.csv"

        print(f"  Input: {input_csv}")
        print(f"  Harmonised: {harmonised_csv}")
        print(f"  Facts: {facts_csv}")

        # Load configuration
        specification = p["Specification"](str(specification_dir))
        pipeline = p["Pipeline"](
            str(pipeline_dir), self.dataset, specification=specification
        )
        schema = specification.pipeline.get(pipeline.name, {}).get(
            "schema", self.dataset
        )
        intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
        factor_fieldnames = specification.factor_fieldnames()

        # Create logs
        resource = la_name.lower().replace(" ", "_")
        issue_log = p["IssueLog"](dataset=self.dataset, resource=resource)
        operational_issue_log = p["OperationalIssueLog"](
            dataset=self.dataset, resource=resource
        )
        column_field_log = p["ColumnFieldLog"](dataset=self.dataset, resource=resource)
        dataset_resource_log = p["DatasetResourceLog"](
            dataset=self.dataset, resource=resource
        )
        converted_resource_log = p["ConvertedResourceLog"](
            dataset=self.dataset, resource=resource
        )

        # Load organization data
        org_csv = PipelineConfig.download_organisation_csv(cache_dir)
        organisation = p["Organisation"](
            organisation_path=str(org_csv), pipeline_dir=Path(pipeline_dir)
        )
        api = p["API"](specification=specification)

        # Get configuration
        entity_range_min = specification.get_dataset_entity_min(self.dataset)
        entity_range_max = specification.get_dataset_entity_max(self.dataset)
        endpoints = []
        organisations_list = ["government-organisation:D2"]
        entry_date = datetime.now().strftime("%Y-%m-%d")

        # Get pipeline configuration
        skip_patterns = pipeline.skip_patterns(resource, endpoints)
        columns = pipeline.columns(resource, endpoints=endpoints)
        concats = pipeline.concatenations(resource, endpoints=endpoints)
        patches = pipeline.patches(resource=resource, endpoints=endpoints)
        lookups = pipeline.lookups(resource=resource)
        default_fields = pipeline.default_fields(resource=resource, endpoints=endpoints)
        default_values = pipeline.default_values(endpoints=endpoints)
        combine_fields = pipeline.combine_fields(endpoints=endpoints)
        redirect_lookups = pipeline.redirect_lookups()
        migrations = pipeline.migrations()
        config = None
        valid_category_values = api.get_valid_category_values(self.dataset, pipeline)

        if len(organisations_list) == 1:
            default_values["organisation"] = organisations_list[0]
        if entry_date and "entry-date" not in default_values:
            default_values["entry-date"] = entry_date

        field_datatype_map = specification.get_field_datatype_map()
        field_typology_map = specification.get_field_typology_map()
        field_prefix_map = specification.get_field_prefix_map()
        dataset_prefix = specification.dataset_prefix(self.dataset)

        print("  Running 26-phase pipeline with per-phase timing...")

        # Define phase creators
        phase_creators = [
            (
                1,
                "ConvertPhase",
                lambda: p["ConvertPhase"](
                    path=str(input_csv),
                    dataset_resource_log=dataset_resource_log,
                    converted_resource_log=converted_resource_log,
                ),
            ),
            (
                2,
                "NormalisePhase",
                lambda: p["NormalisePhase"](skip_patterns=skip_patterns),
            ),
            (3, "ParsePhase", lambda: p["ParsePhase"]()),
            (
                4,
                "ConcatFieldPhase",
                lambda: p["ConcatFieldPhase"](concats=concats, log=column_field_log),
            ),
            (
                5,
                "FilterPhase-1",
                lambda: p["FilterPhase"](filters=pipeline.filters(resource)),
            ),
            (
                6,
                "MapPhase",
                lambda: p["MapPhase"](
                    fieldnames=intermediate_fieldnames,
                    columns=columns,
                    log=column_field_log,
                ),
            ),
            (
                7,
                "FilterPhase-2",
                lambda: p["FilterPhase"](
                    filters=pipeline.filters(resource, endpoints=endpoints)
                ),
            ),
            (
                8,
                "PatchPhase",
                lambda: p["PatchPhase"](issues=issue_log, patches=patches),
            ),
            (
                9,
                "HarmonisePhase",
                lambda: p["HarmonisePhase"](
                    field_datatype_map=field_datatype_map,
                    issues=issue_log,
                    dataset=self.dataset,
                    valid_category_values=valid_category_values,
                ),
            ),
            (
                10,
                "DefaultPhase",
                lambda: p["DefaultPhase"](
                    default_fields=default_fields,
                    default_values=default_values,
                    issues=issue_log,
                ),
            ),
            (
                11,
                "MigratePhase",
                lambda: p["MigratePhase"](
                    fields=specification.schema_field[schema], migrations=migrations
                ),
            ),
            (
                12,
                "OrganisationPhase",
                lambda: p["OrganisationPhase"](
                    organisation=organisation, issues=issue_log
                ),
            ),
            (
                13,
                "FieldPrunePhase",
                lambda: p["FieldPrunePhase"](
                    fields=specification.current_fieldnames(schema)
                ),
            ),
            (
                14,
                "EntityReferencePhase",
                lambda: p["EntityReferencePhase"](
                    dataset=self.dataset, prefix=dataset_prefix, issues=issue_log
                ),
            ),
            (
                15,
                "EntityPrefixPhase",
                lambda: p["EntityPrefixPhase"](dataset=self.dataset),
            ),
            (
                16,
                "EntityLookupPhase",
                lambda: p["EntityLookupPhase"](
                    lookups=lookups,
                    redirect_lookups=redirect_lookups,
                    issue_log=issue_log,
                    operational_issue_log=operational_issue_log,
                    entity_range=[entity_range_min, entity_range_max],
                ),
            ),
            (
                17,
                "SavePhase-harmonised",
                lambda: p["SavePhase"](
                    str(harmonised_csv),
                    fieldnames=intermediate_fieldnames,
                    enabled=True,
                ),
            ),
            (
                18,
                "EntityPrunePhase",
                lambda: p["EntityPrunePhase"](
                    dataset_resource_log=dataset_resource_log
                ),
            ),
            (
                19,
                "PriorityPhase",
                lambda: p["PriorityPhase"](config=config, providers=organisations_list),
            ),
            (20, "PivotPhase", lambda: p["PivotPhase"]()),
            (
                21,
                "FactCombinePhase",
                lambda: p["FactCombinePhase"](
                    issue_log=issue_log, fields=combine_fields
                ),
            ),
            (22, "FactorPhase", lambda: p["FactorPhase"]()),
            (
                23,
                "FactReferencePhase",
                lambda: p["FactReferencePhase"](
                    field_typology_map=field_typology_map,
                    field_prefix_map=field_prefix_map,
                ),
            ),
            (
                24,
                "FactLookupPhase",
                lambda: p["FactLookupPhase"](
                    lookups=lookups,
                    redirect_lookups=redirect_lookups,
                    issue_log=issue_log,
                    odp_collections=specification.get_odp_collections(),
                ),
            ),
            (25, "FactPrunePhase", lambda: p["FactPrunePhase"]()),
            (
                26,
                "SavePhase-facts",
                lambda: p["SavePhase"](str(facts_csv), fieldnames=factor_fieldnames),
            ),
        ]

        # Run phases with timing
        stream_data = []
        total_start = time.time()

        for phase_num, phase_name, phase_creator in phase_creators:
            phase = phase_creator()
            phase_start = time.time()

            if phase_num == 1:
                output_stream = phase.process(iter([]))
            else:
                output_stream = phase.process(iter(stream_data))

            stream_data = list(output_stream)
            duration = time.time() - phase_start
            output_count = len(stream_data)

            if report:
                metrics = report.add_phase(phase_name, phase_num)
                metrics.duration_seconds = duration
                metrics.output_count = output_count

            if duration > 0.1:
                print(
                    f"    Phase {phase_num:2d}: {phase_name:<25} {duration:8.4f}s  ({output_count:,} rows)"
                )

        total_transform_time = time.time() - total_start
        print(f"  Total transform time: {total_transform_time:.3f}s")

        # Count results
        harmonised_count = (
            sum(1 for _ in open(harmonised_csv)) - 1 if harmonised_csv.exists() else 0
        )
        facts_count = sum(1 for _ in open(facts_csv)) - 1 if facts_csv.exists() else 0
        issue_log.save(str(issue_csv))

        if report:
            report.harmonised_records = harmonised_count
            report.fact_records = facts_count

        return {
            "harmonised": harmonised_count,
            "facts": facts_count,
            "harmonised_path": str(harmonised_csv),
            "facts_path": str(facts_csv),
            "issues_path": str(issue_csv),
            "transform_time": total_transform_time,
        }
