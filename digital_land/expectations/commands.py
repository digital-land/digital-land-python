from .checkpoints.dataset import DatasetCheckpoint
from .checkpoints.converted_resource import ConvertedResourceCheckpoint


def run_dataset_checkpoint(
    dataset_path,
    output_dir,
    dataset,
    typology,
    act_on_critical_error=False,
):
    """
    function to run the expectation checkpoint for a sqlite dataset
    """
    checkpoint = DatasetCheckpoint(dataset_path, dataset, typology)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    if act_on_critical_error:
        checkpoint.act_on_critical_error()


def run_converted_resource_checkpoint(
    converted_resource_path,
    output_dir,
    dataset,
    typology,
    act_on_critical_error=False,
):
    """
    Function to run the expectation checkpoint for a converted resource
    """
    checkpoint = ConvertedResourceCheckpoint(converted_resource_path, dataset, typology)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    if act_on_critical_error:
        checkpoint.act_on_critical_error()
