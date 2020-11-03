# encapsulate the GOV.UK register model, with digital land specification extensions
# we expect to merge this back into https://pypi.org/project/openregister/

import os
import os.path
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
    def __init__(self, item={}):
        self.item = item

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
    key = None
    fieldnames = []
    dirname = "dataset/"
    Item = Item

    def __init__(self, dirname=None):
        if dirname:
            self.dirname = dirname
        self.entries = []
        self.record = {}
        if not self.key:
            self.key = self.register

    def __getitem__(self, key):
        if key not in self.record:
            raise KeyError()
        idxs = self.record[key]
        return [self.entries[i] for i in idxs]

    def add(self, item):
        self.entries.append(item)
        key = item.item[self.key]
        self.record.setdefault(key, [])
        self.record[key].append(len(self.entries) - 1)

    def path(self, path=None):
        return path or os.path.join(self.dirname, self.register + ".csv")

    def load(self, path=None):
        path = self.path(path)
        for row in csv.DictReader(open(path, newline="")):
            self.add(self.Item(row))

    def save(self, path=None):
        path = self.path(path)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        writer = csv.DictWriter(
            open(path, "w"),
            fieldnames=self.fieldnames,
            extrasaction="ignore",
            lineterminator="\r\n",
        )
        writer.writeheader()

        path = os.path.join(self.dirname, self.register + ".csv")
        for entry in self.entries:
            writer.writerow(entry.item)
