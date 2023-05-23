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
from digital_land.phase.lookup import EntityLookupPhase, FactLookupPhase
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
from digital_land.pipeline import run_pipeline
from digital_land.schema import Schema
from digital_land.update import add_source_endpoint


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
    concats = pipeline.concatenations(resource)
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
        OrganisationPhase(organisation=organisation),
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

    features = []
    for entity in (e for e in entities if e["typology"] == "geography"):
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
        geojson_path = os.path.join(flattened_dir, f"{dataset_name}-tmp.geojson")
        with open(geojson_path, "w") as out_geojson:
            out_geojson.write(geojson.dumps(feature_collection))

        rfc7946_geojson_path = os.path.join(flattened_dir, f"{dataset_name}.geojson")

        execute(
            [
                "ogr2ogr",
                "-f",
                "GeoJSON",
                "-lco",
                "RFC7946=YES",
                rfc7946_geojson_path,
                geojson_path,
            ]
        )
        if not os.path.isfile(rfc7946_geojson_path):
            logging.error(
                "Could not generate rfc7946 compliant geojson. Use existing file."
            )
            os.rename(geojson_path, rfc7946_geojson_path)
        else:
            # clear up input geojson file
            if os.path.isfile(geojson_path):
                os.remove(geojson_path)


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


def resource_from_path(path):
    return Path(path).stem


def default_output_path(command, input_path):
    directory = "" if command in ["harmonised", "transformed"] else "var/"
    return f"{directory}{command}/{resource_from_path(input_path)}.csv"
