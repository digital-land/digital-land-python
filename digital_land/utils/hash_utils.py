import os
import hashlib

# Read files in 32MB chunks
_chunk_size = 32 * 1024 * 1024


def hash_directory(dir, exclude=[]):
    """Returns a SHA1 hex digest of the contents of a directory.

    Files are sorted before hashing so the result is stable regardless
    of filesystem ordering. File names are included in the hash so
    renames are detected.

    Args:
        dir: Path to the directory to hash.
        exclude: List of path prefixes (relative to dir) to exclude.

    Raises:
        RuntimeError: If dir does not exist or is not a directory.
    """
    if not os.path.isdir(dir):
        raise RuntimeError(
            f"Can't hash {dir} as it doesn't exist or is not a directory"
        )

    hash = hashlib.sha1()
    all_files = []
    for root, _, files in os.walk(dir):
        rel = os.path.relpath(root, dir)
        if rel == ".":
            rel = ""
        rel_files = [os.path.join(rel, file) for file in files]
        all_files += [f for f in rel_files if not f.startswith(tuple(exclude))]

    all_files.sort()

    for file in all_files:
        hash.update(file.encode() + b"\n")
        with open(os.path.join(dir, file), "rb") as f:
            while True:
                data = f.read(_chunk_size)
                if not data:
                    break
                hash.update(data)

    return hash.digest().hex()
