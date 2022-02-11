import csv
import logging
import itertools
from .phase import Phase


def dump(stream, path=None, f=None):
    logging.debug(f"dump {path} {f}")

    if not f:
        f = open(path, "w", newline="")
    writer = csv.writer(f)

    for block in stream:
        writer.writerow(block["line"])


class DumpPhase(Phase):
    """
    dump stream lines to a file
    """

    def __init__(
        self,
        path=None,
        f=None,
        enabled=True,
    ):
        self.path = path
        self.f = f
        self.enabled = enabled

    def process(self, stream):
        if self.enabled:
            stream, dump_stream = itertools.tee(stream)
            dump(
                dump_stream,
                path=self.path,
                f=self.f,
            )

        yield from stream
