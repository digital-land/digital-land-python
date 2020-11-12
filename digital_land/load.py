#
#  load XLS or CSV file into a UTF-8 CSV stream
#

import csv
import logging
from io import StringIO
from pathlib import Path

import pandas as pd
from cchardet import UniversalDetector


def detect_file_encoding(path):
    with open(path, "rb") as f:
        return detect_encoding(f)


def detect_encoding(f):
    detector = UniversalDetector()
    detector.reset()
    for line in f:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result["encoding"]


def resource_hash_from(path):
    return Path(path).stem


class DictReaderInjectResource(csv.DictReader):
    def __init__(self, resource, *args, **kwargs):
        self.resource = resource
        super().__init__(*args, **kwargs)

    def __next__(self):
        # Inject the resource into each row
        row = super().__next__()
        return {
            "resource": self.resource,
            "row": row,
        }


def load_csv_dict(path, inject_resource=False):
    logging.debug(f"reading csv {path}")
    f = open(path, newline=None)
    return DictReaderInjectResource(resource_hash_from(path), f)


def load_csv(path, encoding="UTF-8"):
    logging.debug(f"trying csv {path}")

    if not encoding:
        encoding = detect_file_encoding(path)

        if not encoding:
            return None

        logging.debug(f"detected encoding {encoding}")

    f = open(path, encoding=encoding, newline=None)
    content = f.read()
    if content.lower().startswith("<!doctype "):
        logging.debug(f"{path} has <!doctype")
        return None

    f.seek(0)

    return reader_with_line(f, resource_hash_from(path))


def load_excel(path):
    logging.debug(f"trying excel {path}")
    try:
        excel = pd.read_excel(path)
    except:  # noqa: E722
        return None

    string = excel.to_csv(
        index=None, header=True, encoding="utf-8", quoting=csv.QUOTE_ALL
    )
    f = StringIO(string)

    return reader_with_line(f, resource_hash_from(path))


def reader_with_line(f, resource):
    for line in csv.reader(f):
        yield {
            "resource": resource,
            "line": line,
        }


def load(path):
    return load_csv(path, encoding=None) or load_excel(path)
