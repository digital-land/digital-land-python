import itertools
from .phase import Phase


class ConcatFieldPhase(Phase):

    """
    concatenate fields
    """

    def __init__(self, concats={}):
        self.concats = concats

    def process(self, stream):
        for block in stream:
            row = block["row"]
            o = {}
            for fieldname, cat in self.concats.items():
                o[fieldname] = cat["separator"].join(
                    filter(
                        None,
                        itertools.chain(
                            [o.get(fieldname, None)],
                            [
                                row[h]
                                for h in cat["fields"]
                                if h in row and row[h].strip() != ""
                            ],
                        ),
                    )
                )

            block["row"] = o
            yield block
