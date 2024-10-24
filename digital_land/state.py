import os
from pathlib import Path
import pygit2
import hashlib


class State:
    pass

    def get_dir_hash(self, dir, exclude=[]):
        # SHA1 - good enough for git, good enough for us
        hash = hashlib.sha1()
        all_files = []
        for root, _, files in os.walk(dir):
            rel = os.path.relpath(root, dir)
            print(rel)
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

    def get_code_hash(self):
        code_dir = Path(__file__).parent.parent.absolute()
        if not os.path.exists(os.path.join(code_dir, ".git")):
            raise RuntimeError("No .git directory found")

        repo = pygit2.Repository(__file__)
        commit = repo.revparse_single("HEAD")
        return str(commit.id)
