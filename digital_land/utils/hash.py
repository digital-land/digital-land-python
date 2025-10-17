""" a set of functions which cann be used to generate hashes which is a common need across the project"""

import hashlib
from pathlib import Path


def hash_dir(dir: Path, exclude=[], chunk_size=32 * 1024 * 1024) -> str:
    # check directory is a path
    dir = Path(dir)
    if not (dir.exists() and dir.is_dir()):
        raise RuntimeError(
            f"Can't hash {str(dir)} as it doesn't exist of is not a directory"
        )

    # SHA1 - good enough for git, good enough for us
    hash = hashlib.sha1()

    # get all files
    files = [p for p in dir.rglob("*") if p.is_file() and p.name not in exclude]
    # sort for oder consistency not sure if this is needed
    files.sort()

    for file in files:
        # Add the filename, in case a file is renamed
        hash.update(file.name.encode() + b"\n")
        with open(file, "rb") as h:
            while True:
                data = h.read(chunk_size)
                if not data:
                    break
                hash.update(data)

    return hash.digest().hex()
