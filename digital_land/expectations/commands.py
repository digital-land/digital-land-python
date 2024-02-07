from .checkpoints.dataset import DatasetCheckpoint
from .checkpoints.converted_resource import CovertedResourceCheckpoint
from digital_land.specification import Specification
from pathlib import Path


def run_dataset_checkpoint(
    checkpoint_class,
    dataset_path,
    output_dir,
    spec: Specification,
    dataset=None,
    # act_on_critical_error=True,
):
    print("Dataset_path:", dataset_path)
    """
    function to run the expectation checkpoint for a sqlite dataset
    """
    if not dataset:
        dataset = Path(dataset_path).stem
    typology = spec.get_dataset_typology(dataset)
    checkpoint = checkpoint_class(dataset_path, dataset, typology)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    # if act_on_critical_error:
    #    checkpoint.act_on_critical_error()


def run_coverted_resource_checkpoint(
    converted_resource_path,
    output_dir,
    dataset,
    spec: Specification,
    act_on_critical_error=True,
):
    """
    Function to run the expectation checkpoint for a converted resource
    """
    typology = spec.get_dataset_typology(dataset)
    checkpoint = CovertedResourceCheckpoint(converted_resource_path, dataset, typology)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    if act_on_critical_error:
        checkpoint.act_on_critical_error()
