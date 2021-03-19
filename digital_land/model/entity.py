from collections import ChainMap
from datetime import datetime


class Entity:
    def __init__(self, entries):
        # having the most recent entries at the front of the list means that
        # they take precedance over late items in the ChainMap
        self.entries = sorted(entries, reverse=True)

    def snapshot(self, snapshot_date: datetime = None):
        if snapshot_date:
            return NotImplementedError("snapshot_date not yet implemented")

        return dict(ChainMap(*[entry.data for entry in self.entries]))
