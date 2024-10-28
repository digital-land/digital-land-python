from .checkpoints.dataset import DatasetCheckpoint
from .checkpoints.converted_resource import CovertedResourceCheckpoint

from digital_land.configuration.main import Config


# in principle the command needs a few things:
# an output directory to store expectation logs in this maybe should be in the general log dir
# the dataset being tested, could get fromm the name of the file but should be supplied incase name changes
# right now typology may not but coulld be used later
# act on critical error is optional
def run_dataset_checkpoint(
    file_path,
    output_dir,
    dataset,
    config: Config,
    act_on_critical_error=False,
):
    """
    function to run expectation rules for a given dataset from rules stored in the configuration
    """
    rules = Config.get_expectation_rules(dataset)
    checkpoint = DatasetCheckpoint(file_path, dataset)

    checkpoint.load(rules)
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    if act_on_critical_error:
        checkpoint.act_on_critical_error()


# let's keep checkpoint for now as something to run a checkpoint is able to
# take a set of expectation rules, prroduce a set of expectations and apply them to a dataset
# expectations and other things will need to come directly from the config and spec, at some point
# could make
# need to add expect rules to config
# change expectation_functions to operation
# buiuld operations for dataset checkpoint
# remove issues and other nonsense and replace with an expectation log i.e. it is some output
# to be stored as something produced when the pipline runs. at present it will not
# produce issues in the same way as the pipeline does


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
    checkpoint = CovertedResourceCheckpoint(converted_resource_path, dataset, typology)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(output_dir, format="csv")
    if act_on_critical_error:
        checkpoint.act_on_critical_error()
