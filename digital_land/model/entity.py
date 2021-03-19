from collections import ChainMap
from datetime import datetime


class Entity:
    def __init__(self, entries):
        # having the most recent entries at the front of the list means that
        # they take precedance over later items in the ChainMap
        self.entries = sorted(entries, reverse=True)

    def snapshot(self, snapshot_date: datetime = datetime.now()):
        return dict(
            ChainMap(
                *[
                    entry.data
                    for entry in self.entries
                    if entry.entry_date <= snapshot_date
                ]
            )
        )
