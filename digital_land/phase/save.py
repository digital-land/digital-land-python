import csv
import logging
import itertools
from .phase import Phase


def fsave(reader, f, fieldnames=None):
    if not fieldnames:
        writer = csv.writer(f)
        key = "line"
    else:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        key = "row"

    for row in reader:
        writer.writerow(row[key])


def save(reader, path, fieldnames=None):
    logging.debug(f"saving {path} {fieldnames}")
    with open(path, "w", newline="") as f:
        fsave(reader, f, fieldnames=fieldnames)


class SavePhase(Phase):
    """
    save stream to a file
    """

    def __init__(
        self,
        path,
        fieldnames,
        enabled=True,
    ):
        self.path = path
        self.fieldnames = fieldnames
        self.enabled = enabled

    def process(self, reader):
        if self.enabled:
            reader, save_stream = itertools.tee(reader)
            save(
                save_stream,
                self.path,
                self.fieldnames,
            )

        yield from reader
