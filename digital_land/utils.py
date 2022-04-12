from pathlib import Path


def get_filename_without_suffix(path: Path):
    """
    For pathlib.Path, returns filename without suffix

    :param path: pathlib.Path to get filename of
    :type path: Path
    """
    return path.name[: -len("".join(path.suffixes))]
