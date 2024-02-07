

from digital_land.specification import Specification


def run_expectations(results_path, sqlite_dataset_path, data_quality_yaml):
    from .commands import run_dataset_checkpoint
    from .checkpoints.dataset_checkpoint import DatasetCheckpoint

    # checkpoints = [ OldEntitiesCheckpoint ]
    specification = Specification("specification")


    run_dataset_checkpoint(DatasetCheckpoint, sqlite_dataset_path, results_path, specification)
