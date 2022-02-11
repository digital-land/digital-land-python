import csv
import logging
import itertools
from .phase import Phase


def save(stream, path=None, fieldnames=None, f=None):
    logging.debug(f"save {path} {fieldnames} {f}")

    writer = None

    for block in stream:
        if not fieldnames:
            fieldnames = block["row"].keys()

        if not writer:
            if not f:
                f = open(path, "w", newline="")
            fieldnames = sorted(fieldnames)
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()

        writer.writerow(block["row"])


class SavePhase(Phase):
    """
    save stream rows to a file
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
