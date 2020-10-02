# encapsulate the GOV.UK register model, with digital land specification extensions
# we expect to merge this back into https://pypi.org/project/openregister/

import os
import os.path
import re
import logging
import hashlib
import csv
import json
import canonicaljson


def hash_value(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        logging.info(path)
        with open(path, "wb") as f:
            f.write(data)


class Item:
    def __init__(self):
        self.item = {}

    def migrate(self):
        pass

    def serialise(self):
        return self.item

    def load_json(self, path):
        self.item = json.load(open(path))
        self.migrate()

    def save_json(self, path):
        item = self.serialise()
        data = canonicaljson.encode_canonical_json(item)
        save(path, data)


class Register:
    register = "unknown"
    fieldnames = []

    def __init__(self, entries=[]):
        self.entries = entries

    def add(self, item):
        self.entries.append(item)

    def save(self, path=None):
        if not path:
            path = "dataset/%s.csv" % (self.register)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        writer = csv.DictWriter(
            open(path, "w"), fieldnames=self.fieldnames, extrasaction="ignore"
        )
        writer.writeheader()

        path = os.path.join(self.index_dir, self.register + ".csv")
        for entry in self.entries:
            writer.writerow(entry)
