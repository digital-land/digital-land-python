from .checkpoints.dataset import DatasetCheckpoint

from digital_land.configuration.main import Config
from digital_land.organisation import Organisation


# in principle the command needs a few things:
# an output directory to store expectation logs in this maybe should be in the general log dir
# the dataset being tested, could get fromm the name of the file but should be supplied incase name changes
# right now typology may not but coulld be used later
# act on critical error is optional
def run_dataset_checkpoint(
    dataset,
    file_path,
    output_dir,
    config: Config,
    organisations: Organisation,
    act_on_critical_error=False,
):
    """
    function to run expectation rules for a given dataset from rules stored in the configuration
    requires the comfiguration to be passed in.
    """
    rules = config.get_expectation_rules(dataset)
    print("rules, ", rules)
    checkpoint = DatasetCheckpoint(dataset, file_path, organisations)
    checkpoint.load(rules)
    checkpoint.run()
    checkpoint.save(output_dir)
    # TODO add failure on critical error back in
    if act_on_critical_error:
        checkpoint.act_on_critical_error()
