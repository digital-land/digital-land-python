import os
import pygit2
import json
import hashlib
from datetime import date

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
    ):
        """Build a state object from the current configuration and code"""
        return State(
            {
                "code": State.get_code_hash(),
                "specification": State.get_dir_hash(specification_dir),
                "collection": State.get_dir_hash(
                    collection_dir, ["log/", "log.csv", "pipeline.mk", "resource/"]
                ),
                "resource": State.get_dir_hash(resource_dir),
                "pipeline": State.get_dir_hash(pipeline_dir),
                "incremental_loading_override": incremental_loading_override,
                "last_updated_date": date.today().isoformat(),  # date in YYYY-MM-DD format
                "transform_count": State.get_transform_count(collection_dir),
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

    def get_transform_count(collection_dir):
        """Calculate the number of transformations that need to be completed"""
        from digital_land.collection import Collection

        collection = Collection(directory=collection_dir)
        collection.load(directory=collection_dir)
        dataset_resource = collection.dataset_resource_map()

        # Count total number of transformations (resources across all datasets)
        return sum(len(resources) for resources in dataset_resource.values())


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

    if current == compare:
        return None

    return [i for i in current.keys() if current[i] != compare.get(i, "")]
