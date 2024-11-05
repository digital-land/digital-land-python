import os
import pygit2
import json
import hashlib


class State(dict):
    def __init__(self, data):
        for k, v in data.items():
            self.__setitem__(k, v)

    def build(specification_dir, collection_dir, pipeline_dir):
        """Build a state object from the current configuration and code"""
        return State(
            {
                "code": State.get_code_hash(),
                "specification": State.get_dir_hash(specification_dir),
                "collection": State.get_dir_hash(
                    collection_dir, ["resource/", "log/", "log.csv"]
                ),
                "pipeline": State.get_dir_hash(pipeline_dir),
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
                    data = h.read(256 * 1024)
                    if not data:
                        break
                    hash.update(data)

        return hash.digest().hex()

    def get_code_hash():
        repo = pygit2.Repository(__file__)
        commit = repo.revparse_single("HEAD")
        return str(commit.id)
