# encapsulate the GOV.UK register model, with digital land specification extensions

import hashlib
import json
from canonicaljson import encode_canonical_json
from collections import UserDict, UserList


def hash_value(data):
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class Item(UserDict):
    def pack(self):
        return encode_canonical_json(self.data)

    def unpack(self, data):
        self.data = json.loads(data)
        return self

    def hash(self):
        return hash_value(self.pack())


class Entry(Item):
    "an ordered item in a register"


class Record(UserList):
    "an ordered list of entries sharing the same key value"


class Register:
    def __init__(self, store):
        self.store = store

    def __getitem__(self, value):
        return self.store[value]
