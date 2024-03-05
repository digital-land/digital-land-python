from collections import OrderedDict
import csv
import itertools
import os
import sys
import json
import logging
from pathlib import Path

import geojson
import shapely

from digital_land.specification import Specification
from digital_land.collect import Collector
from digital_land.collection import Collection, resource_path
from digital_land.log import DatasetResourceLog, IssueLog, ColumnFieldLog
from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
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
from digital_land.phase.pivot import PivotPhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase
from digital_land.pipeline import run_pipeline, Lookups, Pipeline
from digital_land.schema import Schema
from digital_land.update import add_source_endpoint
from .register import hash_value


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
    try:
        os.remove(Path(collection_dir) / "log.csv")
        os.remove(Path(collection_dir) / "resource.csv")
    except OSError:
        pass
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    collection.save_csv()


#
#  pipeline commands
#
def convert(input_path, output_path, custom_temp_dir=None):
    if not output_path:
        output_path = default_output_path("converted", input_path)
    dataset_resource_log = DatasetResourceLog()
    run_pipeline(
        ConvertPhase(
            input_path,
            dataset_resource_log=dataset_resource_log,
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
    collection_dir="./collection",  # TBD: remove, replaced by endpoints, organisations and entry_date
    null_path=None,  # TBD: remove this
    issue_dir=None,
    organisation_path=None,
    save_harmonised=False,
    column_field_dir=None,
    dataset_resource_dir=None,
    custom_temp_dir=None,  # TBD: rename to "tmpdir"
    endpoints=[],
    organisations=[],
    entry_date="",
):
    resource = resource_from_path(input_path)
    dataset = dataset
    schema = specification.pipeline[pipeline.name]["schema"]
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    issue_log = IssueLog(dataset=dataset, resource=resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

    # load pipeline configuration
    skip_patterns = pipeline.skip_patterns(resource)
    columns = pipeline.columns(resource, endpoints=endpoints)
    concats = pipeline.concatenations(resource, endpoints=endpoints)
    patches = pipeline.patches(resource=resource)
    lookups = pipeline.lookups(resource=resource)
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=endpoints)
    combine_fields = pipeline.combine_fields(endpoints=endpoints)

    # load organisations
    organisation = Organisation(organisation_path, Path(pipeline.path))

    # load the resource default values from the collection
    if not endpoints:
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        endpoints = collection.resource_endpoints(resource)
        organisations = collection.resource_organisations(resource)
        entry_date = collection.resource_start_date(resource)

    # resource specific default values
    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    if entry_date:
        default_values["entry-date"] = entry_date

    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            custom_temp_dir=custom_temp_dir,
            dataset=dataset,
        ),
        NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_field_log),
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            specification=specification,
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
            specification=specification,
        ),
        EntityPrefixPhase(dataset=dataset),
        EntityLookupPhase(lookups),
        SavePhase(
            default_output_path("harmonised", input_path),
            fieldnames=intermediate_fieldnames,
            enabled=save_harmonised,
        ),
        EntityPrunePhase(
            issue_log=issue_log, dataset_resource_log=dataset_resource_log
        ),
        PivotPhase(),
        FactCombinePhase(issue_log=issue_log, fields=combine_fields),
        FactorPhase(),
        FactReferencePhase(dataset=dataset, specification=specification),
        FactLookupPhase(lookups),
        FactPrunePhase(),
        SavePhase(
            output_path,
            fieldnames=specification.factor_fieldnames(),
        ),
    )

    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))


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
):
    if not output_path:
        print("missing output path", file=sys.stderr)
        sys.exit(2)
    organisation = Organisation(organisation_path, Path(pipeline.path))
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=output_path,
        specification_dir=None,  # TBD: package should use this specification object
    )
    package.create()
    for path in input_paths:
        package.load_transformed(path)
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
        logging.error(f"Can't extract datapackage name from {csv_path}")
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
                ]
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
                    ]
                )
            # clear up input geojson file
            if os.path.isfile(temp_path):
                os.remove(temp_path)


def expectations(results_path, sqlite_dataset_path, data_quality_yaml):
    from digital_land.expectations.main import run_expectation_suite

    run_expectation_suite(results_path, sqlite_dataset_path, data_quality_yaml)


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
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            organisation_path=organisation_path,
            tmp_dir=tmp_dir,
            dataset=dataset,
        )


def resource_from_path(path):
    return Path(path).stem


def default_output_path(command, input_path):
    directory = "" if command in ["harmonised", "transformed"] else "var/"
    return f"{directory}{command}/{resource_from_path(input_path)}.csv"


def assign_entities(
    resource_file_paths,
    collection,
    pipeline_dir,
    specification_dir,
    organisation_path,
    tmp_dir="./var/cache",
    dataset=None,
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

    dataset_resource_map = collection.dataset_resource_map()
    new_lookups = []

    pipeline_name = None
    # establish pipeline if dataset is known - else have to find dataset for each resource
    if dataset is not None:
        pipeline = Pipeline(pipeline_dir, dataset)
        pipeline_name = pipeline.name

    for resource_file_path in resource_file_paths:
        resource = os.path.splitext(os.path.basename(resource_file_path))[0]
        # Find dataset for resource if not given
        if dataset is None:
            for dataset_key, resources in dataset_resource_map.items():
                if resource in list(resources):
                    dataset = dataset_key
                    continue
            # Check whether dataset was found in dataset resource map in case resource hasn't been run through pipeline
            if dataset is not None:
                pipeline = Pipeline(pipeline_dir, dataset)
                pipeline_name = pipeline.name
            else:
                logging.error(
                    "Resource '%s' has not been processed by pipeline - no lookups added"
                    % (resource)
                )
                break

        resource_lookups = get_resource_unidentified_lookups(
            input_path=Path(resource_file_path),
            dataset=dataset,
            organisations=collection.resource_organisations(resource),
            pipeline=pipeline,
            specification=specification,
            tmp_dir=Path(tmp_dir).absolute(),
            org_csv_path=organisation_path,
        )
        new_lookups.append(resource_lookups)

    if pipeline_name is not None:
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
        max_entity_num = lookups.get_max_entity(pipeline_name)
        lookups.entity_num_gen.state["current"] = max_entity_num
        lookups.entity_num_gen.state["range_max"] = (
            specification.get_dataset_entity_max(pipeline_name)
        )
        lookups.entity_num_gen.state["range_min"] = (
            specification.get_dataset_entity_min(pipeline_name)
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
    input_path,
    dataset,
    pipeline,
    specification,
    organisations=[],
    tmp_dir=None,
    org_csv_path=None,
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
    print("----------------------------------------------------------------------")

    # normalise phase inputs
    skip_patterns = pipeline.skip_patterns(resource)
    null_path = None

    # concat field phase
    concats = pipeline.concatenations(resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)

    # map phase
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    columns = pipeline.columns(resource)

    # patch phase
    patches = pipeline.patches(resource=resource)

    # harmonize phase
    issue_log = IssueLog(dataset=dataset, resource=resource)

    # default phase
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=[])

    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    # migrate phase
    schema = specification.pipeline[pipeline.name]["schema"]

    # organisation phase
    organisation = Organisation(org_csv_path, Path(pipeline.path))

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
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            specification=specification,
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
            specification=specification,
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
