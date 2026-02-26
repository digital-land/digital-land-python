import os
import pygit2
import json
import hashlib
from datetime import date
from digital_land.collection import Collection
from digital_land import __version__
from digital_land.utils.dataset_resource_utils import resource_needs_processing

# Read the file in 32MB chunks
_chunk_size = 32 * 1024 * 1024


class State(dict):
    def __init__(self, data):
        for k, v in data.items():
            self.__setitem__(k, v)

    def build(
        specification_dir,
        collection_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        dataset_resource_dir=None,
    ):
        """Build a state object from the current configuration and code"""

        # build collection to get counts
        collection = Collection(directory=collection_dir)
        collection.load(directory=collection_dir)

        # Pre-compute once so counts and state dict share the same values
        specification_hash = State.get_dir_hash(specification_dir)
        pipeline_hash = State.get_dir_hash(pipeline_dir)

        return State(
            {
                "code": State.get_code_hash(),
                "specification": specification_hash,
                "collection": State.get_dir_hash(
                    collection_dir, ["log/", "log.csv", "pipeline.mk", "resource/"]
                ),
                "resource": State.get_dir_hash(resource_dir),
                "pipeline": pipeline_hash,
                "incremental_loading_override": incremental_loading_override,
                "last_updated_date": date.today().isoformat(),  # date in YYYY-MM-DD format
                "transform_count": State.get_transform_count(
                    collection,
                    dataset_resource_dir=dataset_resource_dir,
                    current_code_version=__version__,
                    current_config_hash=pipeline_hash,
                    current_specification_hash=specification_hash,
                ),
                "transform_count_by_dataset": State.get_transform_count_by_dataset(
                    collection,
                    dataset_resource_dir=dataset_resource_dir,
                    current_code_version=__version__,
                    current_config_hash=pipeline_hash,
                    current_specification_hash=specification_hash,
                ),
            }
        )

    def load(path):
        """Build a state object from a previously saved state file"""
        with open(path, "r") as f:
            return State(json.load(f))

    def save(self, output_path):
        """Saves a state object to a file"""
        with open(output_path, "w") as f:
            json.dump(self, f)

    def get_dir_hash(dir, exclude=[]):
        if not os.path.isdir(dir):
            raise RuntimeError(
                f"Can't hash {dir} as it doesn't exist of is not a directory"
            )

        # SHA1 - good enough for git, good enough for us
        hash = hashlib.sha1()
        all_files = []
        for root, _, files in os.walk(dir):
            rel = os.path.relpath(root, dir)
            if rel == ".":
                rel = ""

            rel_files = [os.path.join(rel, file) for file in files]
            all_files += [f for f in rel_files if not f.startswith(tuple(exclude))]

        # The order the files are added will change the hash
        all_files.sort()

        for file in all_files:
            # Add the filename, in case a file is renamed
            hash.update(file.encode() + b"\n")
            with open(os.path.join(dir, file), "rb") as h:
                while True:
                    data = h.read(_chunk_size)
                    if not data:
                        break
                    hash.update(data)

        return hash.digest().hex()

    def get_code_hash():
        repo = pygit2.Repository(__file__)
        commit = repo.revparse_single("HEAD")
        return str(commit.id)

    def get_transform_count(
        collection: Collection,
        dataset_resource_dir=None,
        current_code_version=None,
        current_config_hash=None,
        current_specification_hash=None,
    ):
        """Calculate the number of transformations that need to be completed.

        When dataset_resource_dir is provided, only resources whose existing log
        differs from the current code version, config hash, or specification hash
        are counted. If None, all resources are counted.
        """
        dataset_resource = collection.dataset_resource_map()

        if dataset_resource_dir is None:
            return sum(len(resources) for resources in dataset_resource.values())

        return sum(
            1
            for dataset, resources in dataset_resource.items()
            for resource in resources
            if resource_needs_processing(
                dataset_resource_dir,
                dataset,
                resource,
                current_code_version,
                current_config_hash,
                current_specification_hash,
            )
        )

    def get_transform_count_by_dataset(
        collection: Collection,
        dataset_resource_dir=None,
        current_code_version=None,
        current_config_hash=None,
        current_specification_hash=None,
    ):
        """Calculate the number of transformations needed per dataset.

        When dataset_resource_dir is provided, only resources whose existing log
        differs from the current code version, config hash, or specification hash
        are counted. If None, all resources are counted.
        """
        dataset_resource = collection.dataset_resource_map()
        transform_count_by_dataset = {}
        for dataset, resources in dataset_resource.items():
            if dataset_resource_dir is None:
                transform_count_by_dataset[dataset] = len(resources)
            else:
                transform_count_by_dataset[dataset] = sum(
                    1
                    for resource in resources
                    if resource_needs_processing(
                        dataset_resource_dir,
                        dataset,
                        resource,
                        current_code_version,
                        current_config_hash,
                        current_specification_hash,
                    )
                )
        return transform_count_by_dataset


def compare_state(
    specification_dir,
    collection_dir,
    pipeline_dir,
    resource_dir,
    incremental_loading_override,
    state_path,
):
    """Compares the current state against the one in state_path.
    Returns a list of different elements, or None if they are the same."""
    current = State.build(
        specification_dir=specification_dir,
        collection_dir=collection_dir,
        pipeline_dir=pipeline_dir,
        resource_dir=resource_dir,
        incremental_loading_override=incremental_loading_override,
    )
    # in here current incremental override must be false

    compare = State.load(state_path)
    # we don't want to include whether the previous state was an incremental override in comparison
    current.pop("incremental_loading_override", None)
    compare.pop("incremental_loading_override", None)
    # Also do not want to compare the last_updated_date as it will change every day
    current.pop("last_updated_date", None)
    compare.pop("last_updated_date", None)
    # transform count should not be compared as it changes
    current.pop("transform_count", None)
    compare.pop("transform_count", None)
    # transform count by dataset should not be compared as it changes
    current.pop("transform_count_by_dataset", None)
    compare.pop("transform_count_by_dataset", None)

    if current == compare:
        return None

    return [i for i in current.keys() if current[i] != compare.get(i, "")]
