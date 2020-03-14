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


def load_csv_dict(path):
    logging.debug(f"reading csv {path}")
    return csv.DictReader(open(path, newline=""))


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
