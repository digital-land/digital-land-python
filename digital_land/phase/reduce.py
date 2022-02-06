import logging
from .phase import Phase


class ReducePhase(Phase):
    """
    reduce fields to just those specified for the dataset
    """

    def __init__(self, fields):
        # self.fields = list(set(fields).union(set(("entity"))))
        self.fields = list(set(fields + ["entity", "organisation"]))
        logging.debug(f"reducing fields to {self.fields}")

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]

            o = {}
            for field in self.fields:
                o[field] = row.get(field, "")

            stream_data["row"] = o
            yield stream_data
