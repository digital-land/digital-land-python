from collections import OrderedDict
import csv
import itertools
import os
import sys
import json
import logging
from packaging.version import Version
import pandas as pd
from pathlib import Path

import geojson
import shapely

from digital_land.package.organisation import OrganisationPackage
from digital_land.check import duplicate_reference_check
from digital_land.specification import Specification
from digital_land.collect import Collector
from digital_land.collection import Collection, resource_path
from digital_land.log import (
    DatasetResourceLog,
    IssueLog,
    ColumnFieldLog,
    OperationalIssueLog,
    ConvertedResourceLog,
)
from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
from digital_land.package.datasetparquet import DatasetParquetPackage
from digital_land.phase.combine import FactCombinePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase, execute
from digital_land.phase.default import DefaultPhase
from digital_land.phase.dump import DumpPhase
from digital_land.phase.factor import FactorPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.lookup import (
    EntityLookupPhase,
    FactLookupPhase,
    PrintLookupPhase,
)
from digital_land.phase.map import MapPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.priority import PriorityPhase
from digital_land.phase.pivot import PivotPhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase
from digital_land.pipeline import run_pipeline, Lookups, Pipeline
from digital_land.schema import Schema
from digital_land.update import add_source_endpoint
from digital_land.configuration.main import Config
from digital_land.api import API
from digital_land.state import State

from .register import hash_value
from .utils.gdal_utils import get_gdal_version


logger = logging.getLogger(__name__)


def fetch(url, pipeline):
    """fetch a single source endpoint URL, and add it to the collection"""
    collector = Collector(pipeline.name)
    collector.fetch(url)


def collect(endpoint_path, collection_dir, pipeline):
    """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
    collector = Collector(pipeline.name, Path(collection_dir))
    collector.collect(endpoint_path)


#
#  collection commands
#  TBD: make sub commands
#
def collection_list_resources(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    for resource in sorted(collection.resource.records):
        print(resource_path(resource, directory=collection_dir))


def collection_pipeline_makerules(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    collection.pipeline_makerules()


def collection_save_csv(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    collection.update()
    collection.save_csv()


def operational_issue_save_csv(operational_issue_dir, dataset):
    operationalIssues = OperationalIssueLog(
        operational_issue_dir=operational_issue_dir, dataset=dataset
    )
    operationalIssues.load()
    operationalIssues.update()
    operationalIssues.save_csv()


def collection_retire_endpoints_and_sources(
    config_collections_dir, endpoints_sources_to_retire_csv_path
):
    """
    Retires endpoints and sources based on an input.csv.
    Please note this requires an input csv with the columns: collection, endpoint and source.
    Args:
        config_collections_dir: The directory containing the collections.
        endpoints_sources_to_retire_csv_path: The filepath to the csv containing endpoints and sources to retire.
    """

    try:
        endpoints_sources_to_retire = pd.read_csv(endpoints_sources_to_retire_csv_path)

        to_retire_by_collection = {}

        # Get the unique collection names
        unique_collections = endpoints_sources_to_retire["collection"].unique()

        # Iterate over unique collection names to create dictionary
        for current_collection_name in unique_collections:
            # Filter the DataFrame for the current collection
            collection_df = endpoints_sources_to_retire[
                endpoints_sources_to_retire["collection"] == current_collection_name
            ].copy()

            # Remove the 'collection' column from the filtered DataFrame
            collection_df.drop(columns=["collection"], inplace=True)

            # Store the filtered DataFrame in the dictionary
            to_retire_by_collection[current_collection_name] = collection_df

        # Iterate through collection groups and apply retire_endpoints_and_sources function.
        for collection_name, collection_df_to_retire in to_retire_by_collection.items():
            collection = Collection(
                name=collection_name,
                directory=f"{config_collections_dir}/{collection_name}",
            )
            collection.load()
            collection.retire_endpoints_and_sources(collection_df_to_retire)

    except FileNotFoundError as e:
        print(f"Error: {e}. Please check the file paths and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}.")


#
#  pipeline commands
#
def convert(input_path, output_path, custom_temp_dir=None):
    if not output_path:
        output_path = default_output_path("converted", input_path)
    dataset_resource_log = DatasetResourceLog()
    converted_resource_log = ConvertedResourceLog()
    run_pipeline(
        ConvertPhase(
            input_path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            custom_temp_dir=custom_temp_dir,
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
    column_field_dir=None,
    dataset_resource_dir=None,
    converted_resource_dir=None,
    custom_temp_dir=None,  # TBD: rename to "tmpdir"
    endpoints=[],
    organisations=[],
    entry_date="",
    config_path="var/cache/config.sqlite3",
    resource=None,
    output_log_dir=None,
):
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
    valid_category_values = api.get_valid_category_values(dataset)

    # resource specific default values
    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    if entry_date:
        default_values["entry-date"] = entry_date

    # TODO Migrate all of this into a function in the Pipeline function
    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            custom_temp_dir=custom_temp_dir,
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
        FactLookupPhase(lookups=lookups, redirect_lookups=redirect_lookups),
        FactPrunePhase(),
        SavePhase(
            output_path,
            fieldnames=specification.factor_fieldnames(),
        ),
    )

    issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)

    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    issue_log.save_parquet(os.path.join(output_log_dir, "issue/"))
    operational_issue_log.save(output_dir=operational_issue_dir)
    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))
    converted_resource_log.save(os.path.join(converted_resource_dir, resource + ".csv"))


#
#  build dataset from processed resources
#
def dataset_create(
    input_paths,
    output_path,
    organisation_path,
    pipeline,
    dataset,
    specification,
    issue_dir="issue",
    column_field_dir="var/column-field",
    dataset_resource_dir="var/dataset-resource",
    cache_dir="var/cache/parquet",
):
    if not output_path:
        print("missing output path", file=sys.stderr)
        sys.exit(2)

    # Set up initial objects
    column_field_dir = Path(column_field_dir)
    dataset_resource_dir = Path(dataset_resource_dir)
    organisation = Organisation(
        organisation_path=organisation_path, pipeline_dir=Path(pipeline.path)
    )
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=output_path,
        specification_dir=None,  # TBD: package should use this specification object
    )
    package.create()
    for path in input_paths:
        path_obj = Path(path)
        package.load_transformed(path)
        package.load_column_fields(column_field_dir / dataset / path_obj.name)
        package.load_dataset_resource(dataset_resource_dir / dataset / path_obj.name)
    package.load_entities()

    old_entity_path = os.path.join(pipeline.path, "old-entity.csv")
    if os.path.exists(old_entity_path):
        package.load_old_entities(old_entity_path)

    issue_paths = os.path.join(issue_dir, dataset)
    if os.path.exists(issue_paths):
        for issue_path in os.listdir(issue_paths):
            package.load_issues(os.path.join(issue_paths, issue_path))
    else:
        logging.warning("No directory for this dataset in the provided issue_directory")

    package.add_counts()

    # Repeat for parquet
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    if "dataset" in output_path:
        output_path = output_path.replace("dataset/", f"{cache_dir}/")
    if output_path.endswith((".sqlite", ".sqlite3")):
        output_path = str(Path(output_path).with_suffix(""))
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pqpackage = DatasetParquetPackage(
        dataset,
        path=output_path,
        input_paths=input_paths,
        specification_dir=None,  # TBD: package should use this specification object
    )
    pqpackage.create_temp_table(input_paths)
    pqpackage.load_facts(input_paths, output_path)
    pqpackage.load_fact_resource(input_paths, output_path)
    pqpackage.load_entities(input_paths, output_path, organisation_path)
    pqpackage.pq_to_sqlite(output_path)
    pqpackage.close_conn()


def dataset_dump(input_path, output_path):
    cmd = f"sqlite3 -header -csv {input_path} 'select * from entity;' > {output_path}"
    logging.info(cmd)
    os.system(cmd)


def dataset_dump_flattened(csv_path, flattened_dir, specification, dataset):
    if isinstance(csv_path, str):
        path = Path(csv_path)
        dataset_name = path.stem
    elif isinstance(csv_path, Path):
        dataset_name = csv_path.stem
    else:
        logging.error(f"Can't extract  datapackage name from {csv_path}")
        sys.exit(-1)

    flattened_csv_path = os.path.join(flattened_dir, f"{dataset_name}.csv")
    with open(csv_path, "r") as read_file, open(flattened_csv_path, "w+") as write_file:
        reader = csv.DictReader(read_file)

        spec_field_names = [
            field
            for field in itertools.chain(
                *[
                    specification.current_fieldnames(schema)
                    for schema in specification.dataset_schema[dataset]
                ]
            )
        ]
        reader_fieldnames = [
            field.replace("_", "-")
            for field in list(reader.fieldnames)
            if field != "json"
        ]

        flattened_field_names = set(spec_field_names).difference(set(reader_fieldnames))
        # Make sure we put flattened fieldnames last
        field_names = reader_fieldnames + sorted(list(flattened_field_names))

        writer = csv.DictWriter(write_file, fieldnames=field_names)
        writer.writeheader()
        entities = []
        for row in reader:
            row.pop("geojson", None)
            row = OrderedDict(row)
            json_string = row.pop("json") or "{}"
            row.update(json.loads(json_string))
            kebab_case_row = dict(
                [(key.replace("_", "-"), val) for key, val in row.items()]
            )
            writer.writerow(kebab_case_row)
            entities.append(kebab_case_row)

    # write the entities to json file as well
    flattened_json_path = os.path.join(flattened_dir, f"{dataset_name}.json")
    with open(flattened_json_path, "w") as out_json:
        out_json.write(json.dumps({"entities": entities}))
    batch_size = 100000
    temp_geojson_files = []
    geography_entities = [e for e in entities if e["typology"] == "geography"]
    for i in range(0, len(geography_entities), batch_size):
        batch = geography_entities[i : i + batch_size]
        feature_collection = process_data_in_batches(batch, flattened_dir, dataset_name)

        geojson_path = os.path.join(flattened_dir, f"{dataset_name}-tmp-{i}.geojson")
        temp_geojson_files.append(geojson_path)
        try:
            with open(geojson_path, "w", encoding="utf-8") as out_geojson:
                out_geojson.write(geojson.dumps(feature_collection))
        except Exception as e:
            logging.error(f"Error writing to GeoJSON file: {e}")

    if all(os.path.isfile(path) for path in temp_geojson_files):
        rfc7946_geojson_path = os.path.join(flattened_dir, f"{dataset_name}.geojson")
        env = (
            dict(os.environ, OGR_GEOJSON_MAX_OBJ_SIZE="0")
            if get_gdal_version() >= Version("3.5.2")
            else os.environ
        )
        for temp_path in temp_geojson_files:
            responseCode, _, _ = execute(
                [
                    "ogr2ogr",
                    "-f",
                    "GeoJSON",
                    "-lco",
                    "RFC7946=YES",
                    "-append",
                    rfc7946_geojson_path,
                    temp_path,
                ],
                env=env,
            )

            if responseCode != 0:
                logging.error(
                    "Could not generate rfc7946 compliant geojson. Use existing file."
                )
                execute(
                    [
                        "ogr2ogr",
                        "-f",
                        "GeoJSON",
                        "-append",
                        rfc7946_geojson_path,
                        temp_path,
                    ],
                    env=env,
                )
            # clear up input geojson file
            if os.path.isfile(temp_path):
                os.remove(temp_path)


#
#  configuration commands
#
def collection_add_source(entry, collection, endpoint_url, collection_dir):
    """
    followed by a sequence of optional name and value pairs including the following names:
    "attribution", "licence", "pipelines", "status", "plugin",
    "parameters", "start-date", "end-date"
    """
    entry["collection"] = collection
    entry["endpoint-url"] = endpoint_url
    allowed_names = set(
        list(Schema("endpoint").fieldnames) + list(Schema("source").fieldnames)
    )
    for key in entry.keys():
        if key not in allowed_names:
            logging.error(f"unrecognised argument '{key}'")
            sys.exit(2)
    add_source_endpoint(entry, directory=collection_dir)


def add_endpoints_and_lookups(
    csv_file_path,
    collection_name,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    tmp_dir="./var/cache",
):
    """
    :param csv_file_path:
    :param collection_name:
    :param collection_dir:
    :param pipeline_dir:
    :param specification_dir:
    :param organisation_path:
    :param tmp_dir:
    :return:
    """

    expected_cols = [
        "pipelines",
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "licence",
    ]

    licence_csv_path = os.path.join(specification_dir, "licence.csv")
    valid_licenses = []
    with open(licence_csv_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        valid_licenses = [row["licence"] for row in reader]

    # need to get collection name from somewhere
    # collection name is NOT the dataset name
    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    # read and process each record of the new endpoints csv at csv_file_path
    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        # validate the columns
        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        # this is not perfect we should riase validation errors in our code and below should include a try and except statement
        endpoints = []
        for row in reader:
            if row["licence"] not in valid_licenses:
                raise ValueError(
                    f"Licence '{row['licence']}' is not a valid licence according to the specification."
                )
            if not row["documentation-url"].strip():
                raise ValueError(
                    "The 'documentation-url' must be populated for each row."
                )
            if collection.add_source_endpoint(row):
                endpoint = {
                    "endpoint-url": row["endpoint-url"],
                    "endpoint": hash_value(row["endpoint-url"]),
                    "end-date": row.get("end-date", ""),
                    "plugin": row.get("plugin"),
                    "licence": row["licence"],
                }
                endpoints.append(endpoint)

    # endpoints have been added now lets collect the resources using the endpoint information
    collector = Collector(collection_dir=collection_dir)

    for endpoint in endpoints:
        collector.fetch(
            url=endpoint["endpoint-url"],
            endpoint=endpoint["endpoint"],
            end_date=endpoint["end-date"],
            plugin=endpoint["plugin"],
        )
    # reload log items
    collection.load_log_items()

    dataset_resource_map = collection.dataset_resource_map()

    #  searching for the specific resources that we have downloaded
    for dataset in dataset_resource_map:
        resources_to_assign = []
        for resource in dataset_resource_map[dataset]:
            resource_endpoints = collection.resource_endpoints(resource)
            if any(
                endpoint in [new_endpoint["endpoint"] for new_endpoint in endpoints]
                for endpoint in resource_endpoints
            ):
                resource_file_path = Path(collection_dir) / "resource" / resource
                resources_to_assign.append(resource_file_path)
        assign_entities(
            resource_file_paths=resources_to_assign,
            collection=collection,
            dataset=dataset,
            organisation=collection.resource_organisations(resource),
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            organisation_path=organisation_path,
            endpoints=resource_endpoints,
            tmp_dir=tmp_dir,
        )


def resource_from_path(path):
    return Path(path).stem


def default_output_path(command, input_path):
    directory = "" if command in ["harmonised", "transformed"] else "var/"
    return f"{directory}{command}/{resource_from_path(input_path)}.csv"


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
    print("New Lookups")
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
            lookups.add_entry(entry[0])

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

    collection.save_csv()
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
    custom_temp_dir = tmp_dir  # './var'

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
            custom_temp_dir=custom_temp_dir,
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
        ),
        EntityPrefixPhase(dataset=dataset),
        print_lookup_phase,
    )

    return print_lookup_phase.new_lookup_entries


def process_data_in_batches(entities, flattened_dir, dataset_name):
    features = []
    feature_collection = ""
    for entity in entities:
        geom = entity.pop("geometry")
        point = entity.pop("point")
        if geom:
            try:
                geometry = shapely.wkt.loads(geom)
                feature = geojson.Feature(geometry=geometry, properties=entity)
                features.append(feature)
            except Exception as e:
                logging.error(f"Error loading wkt from entity {entity['entity']}")
                logging.error(e)
        elif point:
            try:
                geometry = shapely.wkt.loads(point)
                feature = geojson.Feature(geometry=geometry, properties=entity)
                features.append(feature)
            except Exception as e:
                logging.error(f"Error loading wkt from entity {entity['entity']}")
                logging.error(e)
        else:
            logging.error(
                f"No geometry or point data for entity {entity['entity']} with typology 'geography'"
            )

    if features:
        feature_collection = geojson.FeatureCollection(
            features=features, name=dataset_name
        )

    return feature_collection


def add_redirections(csv_file_path, pipeline_dir):
    """
    :param csv_file_path:
    :param pipeline_dir:
    :return:
    """
    expected_cols = [
        "entity_source",
        "entity_destination",
    ]

    old_entity_path = Path(pipeline_dir) / "old-entity.csv"

    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        fieldnames = ["old-entity", "status", "entity"]

        f = open(old_entity_path, "a", newline="")
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0:
            writer.writeheader()

        for row in reader:
            if row["entity_source"] == "" or row["entity_destination"] == "":
                print(
                    "Missing entity number for",
                    (
                        row["entity_destination"]
                        if row["entity_source"] == ""
                        else row["entity_source"]
                    ),
                )
            else:
                writer.writerow(
                    {
                        "old-entity": row["entity_source"],
                        "status": "301",
                        "entity": row["entity_destination"],
                    }
                )
    print("Redirections added to old-entity.csv")


def organisation_create(**kwargs):
    package = OrganisationPackage(**kwargs)
    package.create()


def organisation_check(**kwargs):
    output_path = kwargs.pop("output_path")
    lpa_path = kwargs.pop("lpa_path")
    package = OrganisationPackage(**kwargs)
    package.check(lpa_path, output_path)


def save_state(specification_dir, collection_dir, pipeline_dir, output_path):
    state = State.build(
        specification_dir=specification_dir,
        collection_dir=collection_dir,
        pipeline_dir=pipeline_dir,
    )
    state.save(
        output_path=output_path,
    )


def check_state(specification_dir, collection_dir, pipeline_dir, state_path):
    return State.build(
        specification_dir=specification_dir,
        collection_dir=collection_dir,
        pipeline_dir=pipeline_dir,
    ) == State.load(state_path)
