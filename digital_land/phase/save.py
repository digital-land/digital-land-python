import csv
import logging
import itertools
from .phase import Phase


def save(stream, path=None, f=None, fieldnames=None):
    logging.debug(f"save {path} {f} {fieldnames}")

    if not f:
        f = open(path, "w", newline="")

    block = next(stream)
    fieldnames = fieldnames or block.get(fieldnames, "") or block["row"].keys()

    if not fieldnames:
        # write unparsed CSV
        writer = csv.writer(f)
        row = "line"
    else:
        # write canonical CSV
        fieldnames = sorted(fieldnames)
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        row = "row"

    writer.writerow(block[row])
    for block in stream:
        writer.writerow(block[row])


class SavePhase(Phase):
    """
    save stream to a file
    """

    def __init__(
        self,
        path=None,
        f=None,
        fieldnames=None,
        enabled=True,
    ):
        self.path = path
        self.f = f
        self.fieldnames = fieldnames
        self.enabled = enabled

    def process(self, stream):
        if self.enabled:
            stream, save_stream = itertools.tee(stream)
            save(
                save_stream,
                path=self.path,
                f=self.f,
                fieldnames=self.fieldnames,
            )

        yield from stream
