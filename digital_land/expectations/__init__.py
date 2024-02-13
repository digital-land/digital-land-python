import os
from digital_land.specification import Specification


def is_checkpoint(filename):
    filebase, ext = os.path.splitext(filename)
    return filebase.endswith("_checkpoint") and ext == ".py"


def run_expectations(results_path, sqlite_dataset_path, checkpoint):
    for _, _, filenames in os.walk(
        os.path.join(os.path.dirname(__file__), "checkpoints")
    ):
        for filename in filter(is_checkpoint, filenames):
            __import__(
                "checkpoints." + os.path.splitext(filename)[0],
                locals=locals(),
                globals=globals(),
                level=1,
            )

    from .commands import run_dataset_checkpoint

    if checkpoint == "dataset":
        specification = Specification("specification")
        run_dataset_checkpoint(sqlite_dataset_path, results_path, specification)
