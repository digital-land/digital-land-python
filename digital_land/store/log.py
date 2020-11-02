# a store of individual log files, as created by the collector

import os
import glob
import re
import logging
import hashlib
import json
import canonicaljson

from .memory import MemoryStore


def hash_value(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def migrate_item(item):
    # default entry-date field
    if "datetime" in item:
        if "entry-date" not in item:
            item["entry-date"] = item["datetime"]
        del item["datetime"]

    # default endpoint value
    if "endpoint" not in item:
        item["endpoint"] = hash_value(item["url"])


def load_json(path):
    item = json.load(open(path))
    migrate_item(item)
    return item


def serialise_item(item):
    item = item.copy()
    del item["endpoint"]
    return item


def save_json(item, path):
    item = serialise_item(item)
    data = canonicaljson.encode_canonical_json(item)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        logging.info(path)
        with open(path, "wb") as f:
            f.write(data)


def check_item_path(item, path):
    m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
    (date, endpoint) = m.groups()

    if not item["entry-date"].startswith(date):
        logging.warning(
            "incorrect date in path %s for entry-date %s" % (path, item["entry-date"])
        )

    if endpoint != item["endpoint"]:
        logging.warning(
            "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
        )


class LogStore(MemoryStore):
    def __init__(self, register, fieldnames=[], key=None, directory="collection/log/"):
        super().__init__(register, fieldnames=fieldnames, key=key)
        self.directory = directory

    def load(self, directory=None):
        directory = directory or self.directory
        for path in sorted(glob.glob("%s/*/*.json" % (directory))):
            item = load_json(path)
            check_path(item, path)
            self.add_entry(item)
