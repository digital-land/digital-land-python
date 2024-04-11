import csv
import logging
import itertools
from .phase import Phase


# TODO: Add contextManager for f
def save(stream, path=None, fieldnames=None, f=None):
    logging.debug(f"save {path} {fieldnames} {f}")

    if fieldnames:
        block = None
    else:
        try:
            block = next(stream)
            fieldnames = block["row"].keys()
        except StopIteration:
            return

    fieldnames = sorted(fieldnames)
    if not f:
        with open(path, "w", newline="") as f:
            _save_to_csv(stream, fieldnames, f, block)
    else:
        _save_to_csv(stream, fieldnames, f, block)


def _save_to_csv(stream, fieldnames, f, block):
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    if block:
        writer.writerow(block["row"])

    for block in stream:
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
