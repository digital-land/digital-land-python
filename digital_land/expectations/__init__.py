from digital_land.specification import Specification


def run_expectations(results_path, sqlite_dataset_path, checkpoint):
    from .commands import run_dataset_checkpoint

    if checkpoint == "dataset":
        specification = Specification("specification")
        run_dataset_checkpoint(
            checkpoint, sqlite_dataset_path, results_path, specification
        )
