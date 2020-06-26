#
#  load XLS or CSV file into a UTF-8 CSV stream
#

from io import StringIO
from cchardet import UniversalDetector
import csv
import pandas as pd
import logging


def detect_encoding(path):
    detector = UniversalDetector()
    detector.reset()
    with open(path, "rb") as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result["encoding"]


def resource_hash_from(path):
    filename = path.split("/")[-1]
    return ".".join(filename.split(".")[0:-1])


class DictReaderInjectResource(csv.DictReader):
    def __init__(self, resource, *args, **kwargs):
        self.resource = resource
        super().__init__(*args, **kwargs)

    def __next__(self):
        # Inject the resource into each row
        row = super().__next__()
        row["resource"] = self.resource
        return row


def load_csv_dict(path, inject_resource=False):
    logging.debug(f"reading csv {path}")
    resource_hash = resource_hash_from(path)
    file = open(path, newline="")

    if inject_resource:
        return DictReaderInjectResource(resource_hash, file)

    return csv.DictReader(file)


def load_csv(path, encoding="UTF-8"):
    logging.debug(f"trying csv {path}")

    if not encoding:
        encoding = detect_encoding(path)

        if not encoding:
            return None

        logging.debug(f"detected encoding {encoding}")

    f = open(path, encoding=encoding, newline="")
    content = f.read()
    if content.lower().startswith("<!doctype "):
        logging.debug(f"{path} has <!doctype")
        return None

    f.seek(0)
    return csv.reader(f)


def load_excel(path):
    logging.debug(f"trying excel {path}")
    try:
        excel = pd.read_excel(path)
    except:  # noqa: E722
        return None

    string = excel.to_csv(index=None, header=True, encoding="utf-8")
    f = StringIO(string)
    return csv.reader(f)


def load(path):
    return load_csv(path, encoding=None) or load_excel(path)
