import itertools
from .phase import Phase


class ConcatFieldPhase(Phase):

    """
    concatenate fields
    """

    def __init__(self, concats={}, log=None):
        self.concats = concats

    def process(self, stream):
        for block in stream:
            row = block["row"]

            for fieldname, cat in self.concats.items():
                row[fieldname] = cat["separator"].join(
                    filter(
                        None,
                        itertools.chain(
                            [row.get(fieldname, None)],
                            [
                                row[h]
                                for h in cat["fields"]
                                if h in row and row[h].strip() != ""
                            ],
                        ),
                    )
                )

            yield block
