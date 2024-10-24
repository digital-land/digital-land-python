import itertools
from .phase import Phase


class ConcatFieldPhase(Phase):
    """
    concatenate fields
    """

    def __init__(self, concats={}, log=None):
        self.concats = concats

        if log:
            for fieldname, cat in self.concats.items():
                log.add(
                    fieldname,
                    cat["prepend"]
                    + cat["separator"].join(cat["fields"])
                    + cat["append"],
                )

    def process(self, stream):
        for block in stream:
            row = block["row"]

            for fieldname, cat in self.concats.items():
                row[fieldname] = (
                    cat["prepend"]
                    + cat["separator"].join(
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
                    + cat["append"]
                )

            yield block
