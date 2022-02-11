import logging
from .phase import Phase


class ReducePhase(Phase):
    """
    reduce fields to just those specified for the dataset
    """

    def __init__(self, fields):
        self.fields = list(set(fields + ["entity", "organisation"]))
        logging.debug(f"reducing fields to {self.fields}")

    def process(self, stream):
        for block in stream:
            row = block["row"]

            o = {}
            for field in self.fields:
                o[field] = row.get(field, "")

            block["row"] = o
            yield block
