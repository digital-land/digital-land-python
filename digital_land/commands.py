from collections import OrderedDict
import csv
import itertools
import os
import shutil
import sys
import json
import logging
from packaging.version import Version
import pandas as pd
from pathlib import Path
from datetime import datetime
from distutils.dir_util import copy_tree
import geojson
from requests import HTTPError
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
from digital_land.package.dataset_parquet import DatasetParquetPackage
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
from digital_land.pipeline.process import convert_tranformed_csv_to_pq
from digital_land.schema import Schema
from digital_land.update import add_source_endpoint
from digital_land.configuration.main import Config
from digital_land.api import API
from digital_land.state import State
from digital_land.utils.add_data_utils import (
    clear_log,
    get_column_field_summary,
    get_entity_summary,
    get_issue_summary,
    is_date_valid,
    is_url_valid,
    get_user_response,
)

from .register import hash_value
from .utils.gdal_utils import get_gdal_version

logger = logging.getLogger(__name__)


def fetch(url, pipeline):
    """fetch a single source endpoint URL, and add it to the collection"""
    collector = Collector(pipeline.name)
    collector.fetch(url)


def collect(endpoint_path, collection_dir, pipeline, refill_todays_logs=False):
    """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
    collector = Collector(pipeline.name, Path(collection_dir))
    collector.log_file_hashes(Path(collection_dir))
    collector.collect(endpoint_path, refill_todays_logs=refill_todays_logs)


#
#  collection commands
#  TBD: make sub commands
#
def collection_list_resources(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    for resource in sorted(collection.resource.records):
        print(resource_path(resource, directory=collection_dir))


def collection_pipeline_makerules(
    collection_dir,
    specification_dir,
    pipeline_dir,
    resource_dir,
    incremental_loading_override,
    state_path=None,
):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    collection.pipeline_makerules(
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path=state_path,
    )


def collection_save_csv(collection_dir, refill_todays_logs=False):
    collection = Collection(name=None, directory=collection_dir)
    collection.load(refill_todays_logs=refill_todays_logs)
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
    cache_dir="var/cache",
    resource_path="collection/resource.csv",
):
    # set level for logging to see what's going on
    logger.setLevel(logging.INFO)
    logging.getLogger("digital_land.package.dataset_parquet").setLevel(logging.INFO)

    # chek all paths are paths
    issue_dir = Path(issue_dir)
    column_field_dir = Path(column_field_dir)
    dataset_resource_dir = Path(dataset_resource_dir)
    cache_dir = Path(cache_dir)
    resource_path = Path(resource_path)

    # get  the transformed files from the cache directory this  is  assumed right now but we may want to be stricter in the future
    transformed_parquet_dir = cache_dir / "transformed_parquet" / dataset

    # create directory for dataset_parquet_package, will create a general provenance one for now
    dataset_parquet_path = cache_dir / "provenance"

    if not output_path:
        print("missing output path", file=sys.stderr)
        sys.exit(2)

    # Set up initial objects
    organisation = Organisation(
        organisation_path=organisation_path, pipeline_dir=Path(pipeline.path)
    )

    # create sqlite dataset packageas before and load inn data that isn't in the parquetpackage yet
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=output_path,
        specification_dir=None,  # TBD: package should use this specification object
    )
    # don't use create as we don't want to create the indexes
    package.create_database()
    package.disconnect()
    for path in input_paths:
        path_obj = Path(path)
        logging.info(f"loading column field log into {output_path}")
        package.load_column_fields(column_field_dir / dataset / f"{path_obj.stem}.csv")
        logging.info(f"loading dataset resource log into {output_path}")
        package.load_dataset_resource(
            dataset_resource_dir / dataset / f"{path_obj.stem}.csv"
        )
    logger.info(f"loading old entities into {output_path}")
    old_entity_path = Path(pipeline.path) / "old-entity.csv"
    if old_entity_path.exists():
        package.load_old_entities(old_entity_path)

    logger.info(f"loading issues into {output_path}")
    issue_paths = issue_dir / dataset
    if issue_paths.exists():
        for issue_path in os.listdir(issue_paths):
            package.load_issues(os.path.join(issue_paths, issue_path))
    else:
        logger.warning("No directory for this dataset in the provided issue_directory")

    # Repeat for parquet
    # Set up cache directory to store parquet files. The sqlite files created from this will be saved in the dataset
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    pqpackage = DatasetParquetPackage(
        dataset,
        path=dataset_parquet_path,
        specification_dir=None,  # TBD: package should use this specification object
        duckdb_path=cache_dir / "overflow.duckdb",
    )
    pqpackage.load_facts(transformed_parquet_dir)
    pqpackage.load_fact_resource(transformed_parquet_dir)
    pqpackage.load_entities(transformed_parquet_dir, resource_path, organisation_path)

    logger.info("loading fact,fact_resource and entity into {output_path}")
    pqpackage.load_to_sqlite(output_path)

    logger.info(f"add indexes to {output_path}")
    package.connect()
    package.create_cursor()
    package.create_indexes()
    package.disconnect()

    logger.info(f"creating dataset package {output_path} counts")
    package.add_counts()


#
#  update dataset from processed new resources
#
def dataset_update(
    input_paths,
    output_path,
    organisation_path,
    pipeline,
    dataset,
    specification,
    issue_dir="issue",
    column_field_dir="var/column-field",
    dataset_resource_dir="var/dataset-resource",
    bucket_name=None,
):
    """
    Updates the current state of the sqlite files being held in S3 with new resources
    """
    if not output_path:
        print("missing output path", file=sys.stderr)
        sys.exit(2)

    if not bucket_name:
        print("Missing bucket name to get sqlite files", file=sys.stderr)
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
    # Copy files from S3 and load into tables
    table_name = dataset
    object_key = output_path
    package.load_from_s3(
        bucket_name=bucket_name, object_key=object_key, table_name=table_name
    )

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


def validate_and_add_data_input(
    csv_file_path, collection_name, collection_dir, specification_dir, organisation_path
):
    expected_cols = [
        "pipelines",
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "licence",
    ]

    specification = Specification(specification_dir)
    organisation = Organisation(organisation_path=organisation_path)

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()
    # ===== FIRST VALIDATION BASED ON IMPORT.CSV INFO
    # - Check licence, url, date, organisation

    # read and process each record of the new endpoints csv at csv_file_path i.e import.csv

    with open(csv_file_path) as new_endpoints_file:
        reader = csv.DictReader(new_endpoints_file)
        csv_columns = reader.fieldnames

        # validate the columns in input .csv
        for expected_col in expected_cols:
            if expected_col not in csv_columns:
                raise Exception(f"required column ({expected_col}) not found in csv")

        for row in reader:
            # validate licence
            if row["licence"] == "":
                raise ValueError("Licence is blank")
            elif not specification.licence.get(row["licence"], None):
                raise ValueError(
                    f"Licence '{row['licence']}' is not a valid licence according to the specification."
                )
            # check if urls are not blank and valid urls
            is_endpoint_valid, endpoint_valid_error = is_url_valid(
                row["endpoint-url"], "endpoint_url"
            )
            is_documentation_valid, documentation_valid_error = is_url_valid(
                row["documentation-url"], "documentation_url"
            )
            if not is_endpoint_valid or not is_documentation_valid:
                raise ValueError(
                    f"{endpoint_valid_error} \n {documentation_valid_error}"
                )

            # if there is no start-date, do we want to populate it with today's date?
            if row["start-date"]:
                valid_date, error = is_date_valid(row["start-date"], "start-date")
                if not valid_date:
                    raise ValueError(error)

            # validate organisation
            if row["organisation"] == "":
                raise ValueError("The organisation must not be blank")
            elif not organisation.lookup(row["organisation"]):
                raise ValueError(
                    f"The given organisation '{row['organisation']}' is not in our valid organisations"
                )

            # validate pipeline(s) - do they exist and are they in the collection
            pipelines = row["pipelines"].split(";")
            for pipeline in pipelines:
                if not specification.dataset.get(pipeline, None):
                    raise ValueError(
                        f"'{pipeline}' is not a valid dataset in the specification"
                    )
                collection_in_specification = specification.dataset.get(
                    pipeline, None
                ).get("collection")
                if collection_name != collection_in_specification:
                    raise ValueError(
                        f"'{pipeline}' does not belong to provided collection {collection_name}"
                    )

    # VALIDATION DONE, NOW ADD TO COLLECTION
    print("======================================================================")
    print("Endpoint and source details")
    print("======================================================================")
    print("Endpoint URL: ", row["endpoint-url"])
    print("Endpoint Hash:", hash_value(row["endpoint-url"]))
    print("Documentation URL: ", row["documentation-url"])
    print()

    endpoints = []
    # if endpoint already exists, it will indicate it and quit function here
    if collection.add_source_endpoint(row):
        endpoint = {
            "endpoint-url": row["endpoint-url"],
            "endpoint": hash_value(row["endpoint-url"]),
            "end-date": row.get("end-date", ""),
            "plugin": row.get("plugin"),
            "licence": row["licence"],
        }
        endpoints.append(endpoint)
    else:
        # We rely on the add_source_endpoint function to log why it couldn't be added
        raise Exception(
            "Endpoint and source could not be added - is this a duplicate endpoint?"
        )

    # if successfully added we can now attempt to fetch from endpoint
    collector = Collector(collection_dir=collection_dir)
    endpoint_resource_info = {}
    for endpoint in endpoints:
        status = collector.fetch(
            url=endpoint["endpoint-url"],
            endpoint=endpoint["endpoint"],
            end_date=endpoint["end-date"],
            plugin=endpoint["plugin"],
        )
        try:
            log_path = collector.log_path(datetime.utcnow(), endpoint["endpoint"])
            with open(log_path, "r") as f:
                log = json.load(f)
        except Exception as e:
            print(
                f"Error: The log file for {endpoint} could not be read from path {log_path}.\n{e}"
            )
            break

        status = log.get("status", None)
        # Raise exception if status is not 200
        if not status or status != "200":
            exception = log.get("exception", None)
            raise HTTPError(
                f"Failed to collect from URL with status: {status if status else exception}"
            )

        # Resource and path will only be printed if downloaded successfully but should only happen if status is 200
        resource = log.get("resource", None)
        if resource:
            resource_path = Path(collection_dir) / "resource" / resource
            print(
                "Resource collected: ",
                resource,
            )
            print(
                "Resource Path is: ",
                resource_path,
            )

        print(f"Log Status for {endpoint['endpoint']}: The status is {status}")
        endpoint_resource_info.update(
            {
                "endpoint": endpoint["endpoint"],
                "resource": log.get("resource"),
                "resource_path": resource_path,
                "pipelines": row["pipelines"].split(";"),
                "organisation": row["organisation"],
                "entry-date": row["entry-date"],
            }
        )

    return collection, endpoint_resource_info


def add_data(
    csv_file_path,
    collection_name,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    cache_dir=None,
):
    # Potentially track a list of files to clean up at the end of session? e.g log file

    # First validate the input .csv and collect from the endpoint
    collection, endpoint_resource_info = validate_and_add_data_input(
        csv_file_path,
        collection_name,
        collection_dir,
        specification_dir,
        organisation_path,
    )
    # At this point the endpoint will have been added to the collection

    # We need to delete the collection log to enable consecutive runs due to collection.load()
    clear_log(collection_dir, endpoint_resource_info["endpoint"])

    # Ask if user wants to proceed
    if not get_user_response(
        "Do you want to continue processing this resource? (yes/no): "
    ):
        return

    if not cache_dir:
        cache_dir = Path("var/cache/")
    else:
        cache_dir = Path(cache_dir)

    add_data_cache_dir = cache_dir / "add_data"

    output_path = (
        add_data_cache_dir
        / "transformed/"
        / (endpoint_resource_info["resource"] + ".csv")
    )

    issue_dir = add_data_cache_dir / "issue/"
    column_field_dir = add_data_cache_dir / "column_field/"
    dataset_resource_dir = add_data_cache_dir / "dataset_resource/"
    converted_resource_dir = add_data_cache_dir / "converted_resource/"
    converted_dir = add_data_cache_dir / "converted/"
    output_log_dir = add_data_cache_dir / "log/"
    operational_issue_dir = add_data_cache_dir / "performance/ " / "operational_issue/"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_dir.mkdir(parents=True, exist_ok=True)
    output_log_dir.mkdir(parents=True, exist_ok=True)
    operational_issue_dir.mkdir(parents=True, exist_ok=True)

    collection.load_log_items()
    for dataset in endpoint_resource_info["pipelines"]:
        print("======================================================================")
        print("Run pipeline")
        print("======================================================================")
        try:
            pipeline_run(
                dataset,
                Pipeline(pipeline_dir, dataset),
                Specification(specification_dir),
                endpoint_resource_info["resource_path"],
                output_path=output_path,
                collection_dir=collection_dir,
                issue_dir=issue_dir,
                operational_issue_dir=operational_issue_dir,
                column_field_dir=column_field_dir,
                dataset_resource_dir=dataset_resource_dir,
                converted_resource_dir=converted_resource_dir,
                organisation_path=organisation_path,
                endpoints=[endpoint_resource_info["endpoint"]],
                organisations=[endpoint_resource_info["organisation"]],
                resource=endpoint_resource_info["resource"],
                output_log_dir=output_log_dir,
                converted_path=os.path.join(
                    converted_dir, endpoint_resource_info["resource"] + ".csv"
                ),
            )
        except Exception as e:
            raise RuntimeError(
                f"Pipeline failed to process resource with the following error: {e}"
            )
        print("======================================================================")
        print("Pipeline successful!")
        print("======================================================================")

        converted_path = os.path.join(
            converted_dir, endpoint_resource_info["resource"] + ".csv"
        )
        if not os.path.isfile(converted_path):
            # The pipeline doesn't convert .csv resources so direct user to original resource
            converted_path = endpoint_resource_info["resource_path"]
        print(f"Converted .csv resource path: {converted_path}")
        print(f"Transformed resource path: {output_path}")

        column_field_summary = get_column_field_summary(
            dataset,
            endpoint_resource_info,
            column_field_dir,
            converted_dir,
            specification_dir,
            pipeline_dir,
        )
        print(column_field_summary)

        issue_summary = get_issue_summary(endpoint_resource_info, issue_dir)
        print(issue_summary)

        entity_summary = get_entity_summary(
            endpoint_resource_info, output_path, dataset, issue_dir, pipeline_dir
        )
        print(entity_summary)

        # Check for unknown entities and assign them
        if (
            "unknown entity" in issue_summary
            or "unknown entity - missing reference" in issue_summary
        ):
            # Ask if user wants to proceed
            print("\nThere are unknown entities")
            if not get_user_response(
                "Do you want to assign entities for this resource? (yes/no): "
            ):
                return

            # Resource has been processed, run assign entities and reprocess

            # Copy pipeline dir to cache dir so we can modify it
            cache_pipeline_dir = add_data_cache_dir / "pipeline"
            copy_tree(str(pipeline_dir), str(cache_pipeline_dir))

            assign_entities(
                resource_file_paths=[endpoint_resource_info["resource_path"]],
                collection=collection,
                dataset=dataset,
                organisation=[endpoint_resource_info["organisation"]],
                pipeline_dir=cache_pipeline_dir,
                specification_dir=specification_dir,
                organisation_path=organisation_path,
                endpoints=[endpoint_resource_info["endpoint"]],
                tmp_dir=add_data_cache_dir,
            )

            pipeline = Pipeline(cache_pipeline_dir, dataset)

            # Now rerun pipeline with new assigned entities
            try:
                pipeline_run(
                    dataset,
                    pipeline,
                    Specification(specification_dir),
                    endpoint_resource_info["resource_path"],
                    output_path=output_path,
                    collection_dir=collection_dir,
                    issue_dir=issue_dir,
                    operational_issue_dir=operational_issue_dir,
                    column_field_dir=column_field_dir,
                    dataset_resource_dir=dataset_resource_dir,
                    converted_resource_dir=converted_resource_dir,
                    organisation_path=organisation_path,
                    endpoints=[endpoint_resource_info["endpoint"]],
                    organisations=[endpoint_resource_info["organisation"]],
                    resource=endpoint_resource_info["resource"],
                    output_log_dir=output_log_dir,
                    converted_path=os.path.join(
                        converted_dir, endpoint_resource_info["resource"] + ".csv"
                    ),
                )
            except Exception as e:
                raise RuntimeError(
                    f"Pipeline failed to process resource with the following error: {e}"
                )

            entity_summary = get_entity_summary(
                endpoint_resource_info,
                output_path,
                dataset,
                issue_dir,
                cache_pipeline_dir,
            )
            print(entity_summary)

            # Check if there are unassigned entities in the issue summary - raise exception if they still exist
            issue_summary = get_issue_summary(endpoint_resource_info, issue_dir)
            if (
                "unknown entity" in issue_summary
                or "unknown entity - missing reference" in issue_summary
            ):
                print(issue_summary)
                raise Exception(
                    f"Unknown entities remain in resource {endpoint_resource_info['resource']} after assigning entities"
                )

            # Ask if user wants to proceed
            if not get_user_response(
                "Do you want to save changes made in this session? (yes/no): "
            ):
                return

            # Save changes to lookup.csv
            shutil.copy(cache_pipeline_dir / "lookup.csv", pipeline_dir / "lookup.csv")
        else:
            # Ask if user wants to proceed
            if not get_user_response(
                "Do you want to save changes made in this session? (yes/no): "
            ):
                return

        # Save changes to collection
        collection.save_csv()


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
        collection.save_csv()


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


def save_state(
    specification_dir,
    collection_dir,
    pipeline_dir,
    resource_dir,
    incremental_loading_override,
    output_path,
):
    state = State.build(
        specification_dir=specification_dir,
        collection_dir=collection_dir,
        pipeline_dir=pipeline_dir,
        resource_dir=resource_dir,
        incremental_loading_override=incremental_loading_override,
    )
    state.save(
        output_path=output_path,
    )
