# checkpoint needs to assemble class state
# it needs to validate inputs specific for that checkpoint
# it then needs to run expectations
# then it needs to be able to save those expectation resultts
# a checkpoint represents the moment in the process where we tell it the
# type of data it is validating and where the data is
# the primary different between checkpoints is how it loads expectations (i.e. where that are loaded from)
from .base import BaseCheckpoint


class CovertedResourceCheckpoint(BaseCheckpoint):
    def load():
        pass
