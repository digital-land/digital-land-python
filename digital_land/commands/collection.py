import logging
import sys
import pandas as pd
from pathlib import Path

from digital_land.collect import Collector
from digital_land.collection import Collection, resource_path
from digital_land.schema import Schema
from digital_land.state import State
from digital_land.update import add_source_endpoint

logger = logging.getLogger(__name__)


def fetch(url, pipeline):
    """fetch a single source endpoint URL, and add it to the collection"""
    collector = Collector(pipeline.name)
    collector.fetch(url)


def collect(endpoint_path, collection_dir, pipeline, refill_todays_logs=False):
    """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
    collector = Collector(pipeline.name, Path(collection_dir))
    collector.collection_dir_file_hashes(Path(collection_dir))
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
