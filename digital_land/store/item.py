# a store of individual item.json files

import os
import glob
import logging

from ..register import Item
from .memory import MemoryStore


class ItemStore(MemoryStore):
    def save_item(self, item, path):
        data = item.pack()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            logging.info("saving %s" % (path))
            with open(path, "wb") as f:
                f.write(data)

    def load_item(self, path):
        data = open(path).read()
        item = Item()
        item.unpack(data)
        return item

    def load(self, directory="."):
        path = "%s/*.json" % (directory)
        logging.debug("loading %s" % (path))
        for path in sorted(glob.glob(path)):
            item = self.load_item(path)
            self.add_entry(item)
