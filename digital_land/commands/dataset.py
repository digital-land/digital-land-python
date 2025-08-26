import logging
import sys
import os
import csv
import itertools
import geojson
import json
import shapely

from pathlib import Path
from collections import OrderedDict
from packaging.version import Version

from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
from digital_land.package.dataset_parquet import DatasetParquetPackage
from digital_land.utils.gdal_utils import get_gdal_version
from digital_land.phase.convert import execute

logger = logging.getLogger(__name__)


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
        transformed_parquet_dir=transformed_parquet_dir,
    )
    # To find facts we have a complex SQL window function that can cause memory issues. To aid the allocation of memory
    # we decide on a parquet strategy, based on how many parquet files we have, the overall size of these
    # files and the available memory. We will look at the following strategies:
    # 1) if we have a small number of files or the total size of the files is small then we can run the SQL over all of
    # these files.
    # 2) Grouping the parquet files into 256MB batches. Then running SQL either on all of these batches at once, or
    # bucketing the data so that we run the window SQL function on a subset of facts (then concatenate them)

    # Group parquet files into approx 256MB batches (if needed)
    if pqpackage.strategy != "direct":
        pqpackage.group_parquet_files(transformed_parquet_dir, target_mb=256)
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
    dataset_path=None,
):
    """
    Updates the current state of the sqlite files being held in S3 with new resources
    `dataset_path` can be passed in to update a local sqlite file instead of downloading from S3.
    """
    if not dataset_path:
        if not output_path:
            print("missing output path", file=sys.stderr)
            sys.exit(2)
        if not bucket_name:
            print("Missing bucket name to get sqlite files", file=sys.stderr)
            sys.exit(2)
    else:
        if not os.path.exists(dataset_path):
            logger.error(f"Local dataset at {dataset_path} not found")
            sys.exit(2)

    # Set up initial objects
    column_field_dir = Path(column_field_dir)
    dataset_resource_dir = Path(dataset_resource_dir)
    organisation = Organisation(
        organisation_path=organisation_path, pipeline_dir=Path(pipeline.path)
    )
    if not dataset_path:
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
    else:
        # Reading from local dataset file
        logging.info(f"Reading from local dataset file {dataset_path}")
        package = DatasetPackage(
            dataset,
            organisation=organisation,
            path=dataset_path,
            specification_dir=None,  # TBD: package should use this specification object
        )
        package.set_up_connection()
        package.load()
        package.disconnect()

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

        try:
            gdal_version = get_gdal_version()
        except Exception as e:
            logger.error(f"Failed to get GDAL version: {e} assuming 3.5.2 or later")
            gdal_version = Version("3.5.2")

        env = (
            dict(os.environ, OGR_GEOJSON_MAX_OBJ_SIZE="0")
            if gdal_version >= Version("3.5.2")
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
                logger.error(
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
