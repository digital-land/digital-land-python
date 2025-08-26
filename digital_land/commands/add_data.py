import csv
import json
import os
import shutil
import pandas as pd

from datetime import datetime
from requests import HTTPError
from pathlib import Path
from distutils.dir_util import copy_tree

from digital_land.specification import Specification
from digital_land.organisation import Organisation
from digital_land.collect import Collector
from digital_land.collection import Collection
from digital_land.pipeline import Pipeline
from digital_land.register import hash_value
from digital_land.utils.add_data_utils import (
    clear_log,
    download_dataset,
    get_column_field_summary,
    is_date_valid,
    is_url_valid,
    get_user_response,
    get_issue_summary,
    get_entity_summary,
    get_transformed_entities,
    get_existing_endpoints_summary,
    get_updated_entities_summary,
)

from .pipeline import assign_entities, pipeline_run
from .dataset import dataset_update


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

    collection.load_log_items()
    for dataset in endpoint_resource_info["pipelines"]:
        pipeline = Pipeline(pipeline_dir, dataset)
        specification = Specification(specification_dir)

        issue_dir = add_data_cache_dir / "issue/" / dataset
        column_field_dir = add_data_cache_dir / "column_field/" / dataset
        dataset_resource_dir = add_data_cache_dir / "dataset_resource/" / dataset
        converted_resource_dir = add_data_cache_dir / "converted_resource/"
        converted_dir = add_data_cache_dir / "converted/"
        output_log_dir = add_data_cache_dir / "log/"
        operational_issue_dir = (
            add_data_cache_dir / "performance/ " / "operational_issue/"
        )
        output_path = (
            add_data_cache_dir
            / "transformed/"
            / dataset
            / (endpoint_resource_info["resource"] + ".csv")
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        issue_dir.mkdir(parents=True, exist_ok=True)
        column_field_dir.mkdir(parents=True, exist_ok=True)
        dataset_resource_dir.mkdir(parents=True, exist_ok=True)
        converted_resource_dir.mkdir(parents=True, exist_ok=True)
        converted_dir.mkdir(parents=True, exist_ok=True)
        output_log_dir.mkdir(parents=True, exist_ok=True)
        operational_issue_dir.mkdir(parents=True, exist_ok=True)
        print("======================================================================")
        print("Run pipeline")
        print("======================================================================")
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

        entities_assigned = False
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

            entities_assigned = True

        # Ask if user wants to proceed
        if not get_user_response(
            "Do you want to save changes made in this session? (yes/no): "
        ):
            return

        # Save changes to collection
        collection.save_csv()
        if entities_assigned:
            # Save changes to lookup.csv
            shutil.copy(cache_pipeline_dir / "lookup.csv", pipeline_dir / "lookup.csv")

        # Now check for existing endpoints for this provision/organisation
        print(
            "\n======================================================================"
        )
        print("Retire old endpoints/sources")
        print("======================================================================")
        existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
            endpoint_resource_info, collection, dataset
        )
        print(existing_endpoints_summary)
        if existing_sources:
            if get_user_response(
                "Do you want to retire any of these existing endpoints? (yes/no): "
            ):
                # iterate over existing sources and ask if they should be retired
                sources_to_retire = []
                for source in existing_sources:
                    if get_user_response(f"{source['endpoint-url']}? (yes/no): "):
                        sources_to_retire.append(source)

                if sources_to_retire:
                    collection.retire_endpoints_and_sources(
                        pd.DataFrame.from_records(sources_to_retire)
                    )

        # Update dataset and view newly updated dataset
        print(
            "\n======================================================================"
        )
        print("Update dataset")
        print("======================================================================")
        if get_user_response(
            f"""\nDo you want to view an updated {dataset} dataset with the newly added data?
            \nNote this requires downloading the dataset if not already done so -
            for some datasets this can take a while \n\n(yes/no): """
        ):
            dataset_path = download_dataset(dataset, specification, cache_dir)
            original_entities = get_transformed_entities(dataset_path, output_path)
            print(f"Updating {dataset}.sqlite3 with new data...")
            dataset_update(
                input_paths=[output_path],
                output_path=None,
                organisation_path=organisation_path,
                pipeline=pipeline,
                dataset=dataset,
                specification=specification,
                issue_dir=os.path.split(issue_dir)[0],
                column_field_dir=os.path.split(column_field_dir)[0],
                dataset_resource_dir=os.path.split(dataset_resource_dir)[0],
                dataset_path=dataset_path,
            )
            updated_entities = get_transformed_entities(dataset_path, output_path)
            updated_entities_summary, diffs_df = get_updated_entities_summary(
                original_entities, updated_entities
            )
            print(updated_entities_summary)
            if diffs_df is not None:
                diffs_path = (
                    add_data_cache_dir
                    / dataset
                    / "diffs"
                    / f"{endpoint_resource_info['resource']}.csv"
                )
                os.makedirs(os.path.dirname(diffs_path))
                diffs_df.to_csv(diffs_path)
                print(f"\nDetailed breakdown found in file: {diffs_path}")


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


def check_and_assign_entities(
    resource_file_paths,
    endpoints,
    collection_name,
    dataset,
    organisation,
    collection_dir,
    organisation_path,
    specification_dir,
    pipeline_dir,
    input_path=None,
):
    # Assigns entities for the given resources in the given collection and run pipeline to get the transformed resource.

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    cache_dir = Path("var/cache/")
    assign_entities_cache_dir = cache_dir / "assign_entities"

    resource_path = resource_file_paths[0]
    resource = Path(resource_path).name
    if input_path:
        output_path = input_path
    else:
        output_path = assign_entities_cache_dir / "transformed/" / f"{resource}.csv"

    issue_dir = assign_entities_cache_dir / "issue/"
    column_field_dir = assign_entities_cache_dir / "column_field/"
    dataset_resource_dir = assign_entities_cache_dir / "dataset_resource/"
    converted_resource_dir = assign_entities_cache_dir / "converted_resource/"
    converted_dir = assign_entities_cache_dir / "converted/"
    output_log_dir = assign_entities_cache_dir / "log/"
    operational_issue_dir = (
        assign_entities_cache_dir / "performance " / "operational_issue/"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_dir.mkdir(parents=True, exist_ok=True)
    output_log_dir.mkdir(parents=True, exist_ok=True)
    operational_issue_dir.mkdir(parents=True, exist_ok=True)

    cache_pipeline_dir = assign_entities_cache_dir / collection.name / "pipeline"
    copy_tree(str(pipeline_dir), str(cache_pipeline_dir))

    new_lookups = assign_entities(
        resource_file_paths,
        collection,
        dataset,
        organisation,
        cache_pipeline_dir,
        specification_dir,
        organisation_path,
        endpoints,
        cache_dir,
    )

    pipeline = Pipeline(cache_pipeline_dir, dataset)
    try:
        pipeline_run(
            dataset,
            pipeline,
            Specification(specification_dir),
            resource_path,
            output_path=output_path,
            collection_dir=collection_dir,
            issue_dir=issue_dir,
            operational_issue_dir=operational_issue_dir,
            column_field_dir=column_field_dir,
            dataset_resource_dir=dataset_resource_dir,
            converted_resource_dir=converted_resource_dir,
            organisation_path=organisation_path,
            endpoints=endpoints,
            organisations=organisation,
            resource=resource,
            output_log_dir=output_log_dir,
            converted_path=os.path.join(converted_dir, resource + ".csv"),
        )
    except Exception as e:
        raise RuntimeError(
            f"Pipeline failed to process resource with the following error: {e}"
        )

    endpoint_resource_info = {
        "resource": resource,
        "organisation": organisation[0],
    }
    new_entities = [entry["entity"] for entry in new_lookups]
    issue_summary = get_issue_summary(endpoint_resource_info, issue_dir, new_entities)
    print(issue_summary)

    if "No issues found" not in issue_summary:
        if not get_user_response(
            "Do you want to continue processing this resource? (yes/no): "
        ):
            return False
    return True
