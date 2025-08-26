import sys
import logging
import os
import csv

from pathlib import Path

from digital_land.log import (
    DatasetResourceLog,
    ConvertedResourceLog,
    OperationalIssueLog,
    IssueLog,
    ColumnFieldLog,
)
from digital_land.api import API
from digital_land.pipeline import (
    run_pipeline,
)
from digital_land.phase import (
    ConvertPhase,
    DumpPhase,
    NormalisePhase,
    ParsePhase,
    ConcatFieldPhase,
    FilterPhase,
    MapPhase,
    PatchPhase,
    HarmonisePhase,
    DefaultPhase,
    MigratePhase,
    OrganisationPhase,
    FieldPrunePhase,
    EntityReferencePhase,
    EntityPrefixPhase,
    EntityLookupPhase,
    SavePhase,
    EntityPrunePhase,
    PriorityPhase,
    PivotPhase,
    FactCombinePhase,
    FactorPhase,
    FactReferencePhase,
    FactLookupPhase,
    FactPrunePhase,
    PrintLookupPhase,
)
from digital_land.configuration.main import Config
from digital_land.organisation import Organisation
from digital_land.collection import Collection
from digital_land.specification import Specification
from digital_land.check import duplicate_reference_check
from digital_land.pipeline.process import convert_tranformed_csv_to_pq
from digital_land.pipeline import Pipeline, Lookups

from .utils import default_output_path, resource_from_path


logger = logging.getLogger(__name__)


def operational_issue_save_csv(operational_issue_dir, dataset):
    operationalIssues = OperationalIssueLog(
        operational_issue_dir=operational_issue_dir, dataset=dataset
    )
    operationalIssues.load()
    operationalIssues.update()
    operationalIssues.save_csv()


def convert(input_path, output_path):
    if not output_path:
        output_path = default_output_path("converted", input_path)
    dataset_resource_log = DatasetResourceLog()
    converted_resource_log = ConvertedResourceLog()
    # TBD this actualy duplictaes the data and does nothing else, should just convert it?
    run_pipeline(
        ConvertPhase(
            input_path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
        ),
        DumpPhase(output_path),
    )
    dataset_resource_log.save(f=sys.stdout)


def pipeline_run(
    dataset,
    pipeline,
    specification,
    input_path,
    output_path,
    collection_dir,  # TBD: remove, replaced by endpoints, organisations and entry_date
    null_path=None,  # TBD: remove this
    issue_dir=None,
    operational_issue_dir="performance/operational_issue/",
    organisation_path=None,
    save_harmonised=False,
    #  TBD save all logs in  a log directory, this will mean only one path passed in.
    column_field_dir=None,
    dataset_resource_dir=None,
    converted_resource_dir=None,
    cache_dir="var/cache",
    endpoints=[],
    organisations=[],
    entry_date="",
    config_path="var/cache/config.sqlite3",
    resource=None,
    output_log_dir=None,
    converted_path=None,
):
    # set up paths
    cache_dir = Path(cache_dir)

    if resource is None:
        resource = resource_from_path(input_path)
    dataset = dataset
    schema = specification.pipeline[pipeline.name]["schema"]
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    issue_log = IssueLog(dataset=dataset, resource=resource)
    operational_issue_log = OperationalIssueLog(dataset=dataset, resource=resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)
    converted_resource_log = ConvertedResourceLog(dataset=dataset, resource=resource)
    api = API(specification=specification)
    entity_range_min = specification.get_dataset_entity_min(dataset)
    entity_range_max = specification.get_dataset_entity_max(dataset)

    # load pipeline configuration
    skip_patterns = pipeline.skip_patterns(resource, endpoints)
    columns = pipeline.columns(resource, endpoints=endpoints)
    concats = pipeline.concatenations(resource, endpoints=endpoints)
    patches = pipeline.patches(resource=resource, endpoints=endpoints)
    lookups = pipeline.lookups(resource=resource)
    default_fields = pipeline.default_fields(resource=resource, endpoints=endpoints)
    default_values = pipeline.default_values(endpoints=endpoints)
    combine_fields = pipeline.combine_fields(endpoints=endpoints)
    redirect_lookups = pipeline.redirect_lookups()

    # load config db
    # TODO get more information from the config
    # TODO in future we need better way of making specification optional for config
    if Path(config_path).exists():
        config = Config(path=config_path, specification=specification)
    else:
        logging.error("Config path  does not exist")
        config = None

    # load organisations
    organisation = Organisation(
        organisation_path=organisation_path, pipeline_dir=Path(pipeline.path)
    )

    # load the resource default values from the collection
    if not endpoints:
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        endpoints = collection.resource_endpoints(resource)
        organisations = collection.resource_organisations(resource)
        entry_date = collection.resource_start_date(resource)

    # Load valid category values
    valid_category_values = api.get_valid_category_values(dataset, pipeline)

    # resource specific default values
    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    # need an entry-date for all entries and for facts
    # if a default entry-date isn't set through config then use the entry-date passed
    # to this function
    if entry_date:
        if "entry-date" not in default_values:
            default_values["entry-date"] = entry_date

    # TODO Migrate all of this into a function in the Pipeline function
    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=converted_path,
        ),
        NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_field_log),
        FilterPhase(filters=pipeline.filters(resource)),
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource, endpoints=endpoints)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            field_datatype_map=specification.get_field_datatype_map(),
            issues=issue_log,
            dataset=dataset,
            valid_category_values=valid_category_values,
        ),
        DefaultPhase(
            default_fields=default_fields,
            default_values=default_values,
            issues=issue_log,
        ),
        # TBD: move migrating columns to fields to be immediately after map
        # this will simplify harmonisation and remove intermediate_fieldnames
        # but effects brownfield-land and other pipelines which operate on columns
        MigratePhase(
            fields=specification.schema_field[schema],
            migrations=pipeline.migrations(),
        ),
        OrganisationPhase(organisation=organisation, issues=issue_log),
        FieldPrunePhase(fields=specification.current_fieldnames(schema)),
        EntityReferencePhase(
            dataset=dataset,
            prefix=specification.dataset_prefix(dataset),
            issues=issue_log,
        ),
        EntityPrefixPhase(dataset=dataset),
        EntityLookupPhase(
            lookups=lookups,
            redirect_lookups=redirect_lookups,
            issue_log=issue_log,
            operational_issue_log=operational_issue_log,
            entity_range=[entity_range_min, entity_range_max],
        ),
        SavePhase(
            default_output_path("harmonised", input_path),
            fieldnames=intermediate_fieldnames,
            enabled=save_harmonised,
        ),
        EntityPrunePhase(dataset_resource_log=dataset_resource_log),
        PriorityPhase(config=config),
        PivotPhase(),
        FactCombinePhase(issue_log=issue_log, fields=combine_fields),
        FactorPhase(),
        FactReferencePhase(
            field_typology_map=specification.get_field_typology_map(),
            field_prefix_map=specification.get_field_prefix_map(),
        ),
        FactLookupPhase(
            lookups=lookups,
            redirect_lookups=redirect_lookups,
            issue_log=issue_log,
            odp_collections=specification.get_odp_collections(),
        ),
        FactPrunePhase(),
        SavePhase(
            output_path,
            fieldnames=specification.factor_fieldnames(),
        ),
    )

    # In the FactCombinePhase, when combine_fields has some values, we check for duplicates and combine values.
    # If we have done this then we will not call duplicate_reference_check as we have already carried out a
    # duplicate check and stop messages appearing in issues about reference values not being unique
    if combine_fields == {}:
        issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)

    issue_log.apply_entity_map()
    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    issue_log.save_parquet(os.path.join(output_log_dir, "issue/"))
    operational_issue_log.save(output_dir=operational_issue_dir)
    if column_field_dir:
        column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))
    converted_resource_log.save(os.path.join(converted_resource_dir, resource + ".csv"))
    # create converted parquet in the var directory
    cache_dir = Path(organisation_path).parent
    transformed_parquet_dir = cache_dir / "transformed_parquet" / dataset
    transformed_parquet_dir.mkdir(exist_ok=True, parents=True)
    convert_tranformed_csv_to_pq(
        input_path=output_path,
        output_path=transformed_parquet_dir / f"{resource}.parquet",
    )


def assign_entities(
    resource_file_paths,
    collection,
    dataset,
    organisation,
    pipeline_dir,
    specification_dir,
    organisation_path,
    endpoints,
    tmp_dir="./var/cache",
):
    """
    Assigns entities for the given resources in the given collection. The resources must have sources already added to the collection
    :param resource_file_paths:
    :param collection:
    :param pipeline_dir:
    :param specification_dir:
    :param organisation_path:
    :param tmp_dir:
    :return:
    """

    specification = Specification(specification_dir)

    print("")
    print("======================================================================")
    print("New Entities")
    print("======================================================================")

    new_lookups = []

    pipeline = Pipeline(pipeline_dir, dataset)
    pipeline_name = pipeline.name

    for resource_file_path in resource_file_paths:
        resource_lookups = get_resource_unidentified_lookups(
            input_path=Path(resource_file_path),
            dataset=dataset,
            organisations=organisation,
            pipeline=pipeline,
            specification=specification,
            tmp_dir=Path(tmp_dir).absolute(),
            org_csv_path=organisation_path,
            endpoints=endpoints,
        )
        new_lookups.append(resource_lookups)

    # save new lookups to file
    lookups = Lookups(pipeline_dir)
    # Check if the lookups file exists, create it if not
    if not os.path.exists(lookups.lookups_path):
        with open(lookups.lookups_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(list(lookups.schema.fieldnames))

    lookups.load_csv()
    for new_lookup in new_lookups:
        for idx, entry in enumerate(new_lookup):
            lookup = entry[0]
            if "reference" in lookup and isinstance(lookup["reference"], str):
                lookup["reference"] = lookup["reference"].strip('"')
            lookups.add_entry(lookup)

    # save edited csvs
    max_entity_num = lookups.get_max_entity(pipeline_name, specification)
    lookups.entity_num_gen.state["current"] = max_entity_num
    lookups.entity_num_gen.state["range_max"] = specification.get_dataset_entity_max(
        pipeline_name
    )
    lookups.entity_num_gen.state["range_min"] = specification.get_dataset_entity_min(
        pipeline_name
    )

    # TO DO: Currently using pipeline_name to find dataset min, max, current
    # This would not function properly if each resource had a different dataset

    new_lookups = lookups.save_csv()

    for entity in new_lookups:
        print(
            entity["prefix"],
            ",",
            entity["organisation"],
            ",",
            entity["reference"],
            ",",
            entity["entity"],
        )
    return new_lookups


def get_resource_unidentified_lookups(
    input_path: Path,
    dataset: str,
    pipeline: Pipeline,
    specification: Specification,
    organisations: list = [],
    tmp_dir: Path = None,
    org_csv_path: Path = None,
    endpoints: list = [],
):
    # convert phase inputs
    # could alter resource_from_path to file from path and promote to a utils folder
    resource = resource_from_path(input_path)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

    print("")
    print("----------------------------------------------------------------------")
    print(f">>> organisations:{organisations}")
    print(f">>> resource:{resource}")
    print(f">>> endpoints:{endpoints}")
    print(f">>> dataset:{dataset}")
    print("----------------------------------------------------------------------")

    # normalise phase inputs
    skip_patterns = pipeline.skip_patterns(resource, endpoints)
    null_path = None

    # concat field phase
    concats = pipeline.concatenations(resource, endpoints)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # map phase
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    columns = pipeline.columns(resource, endpoints)

    # patch phase
    patches = pipeline.patches(resource=resource, endpoints=endpoints)

    # harmonize phase
    issue_log = IssueLog(dataset=dataset, resource=resource)

    # default phase
    default_fields = pipeline.default_fields(resource=resource, endpoints=endpoints)
    default_values = pipeline.default_values(endpoints=[])

    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    # migrate phase
    schema = specification.pipeline[pipeline.name]["schema"]

    # organisation phase
    organisation = Organisation(
        organisation_path=org_csv_path, pipeline_dir=Path(pipeline.path)
    )

    # print lookups phase
    pipeline_lookups = pipeline.lookups()
    redirect_lookups = pipeline.redirect_lookups()
    print_lookup_phase = PrintLookupPhase(
        lookups=pipeline_lookups, redirect_lookups=redirect_lookups
    )

    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
        ),
        NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_field_log),
        FilterPhase(filters=pipeline.filters(resource)),
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource, endpoints=endpoints)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            field_datatype_map=specification.get_field_datatype_map(),
            issues=issue_log,
        ),
        DefaultPhase(
            default_fields=default_fields,
            default_values=default_values,
            issues=issue_log,
        ),
        # TBD: move migrating columns to fields to be immediately after map
        # this will simplify harmonisation and remove intermediate_fieldnames
        # but effects brownfield-land and other pipelines which operate on columns
        MigratePhase(
            fields=specification.schema_field[schema],
            migrations=pipeline.migrations(),
        ),
        OrganisationPhase(organisation=organisation, issues=issue_log),
        FieldPrunePhase(fields=specification.current_fieldnames(schema)),
        EntityReferencePhase(
            dataset=dataset,
            prefix=specification.dataset_prefix(dataset),
            issues=issue_log,
        ),
        EntityPrefixPhase(dataset=dataset),
        print_lookup_phase,
    )

    return print_lookup_phase.new_lookup_entries
