from collections import ChainMap
from datetime import date


class Entity:
    def __init__(self, entries):
        # having the most recent entries at the front of the list means that
        # they take precedance over later items in the ChainMap
        self.entries = sorted(entries, reverse=True)

    def snapshot(self, snapshot_date: date = date.today()):
        return dict(
            ChainMap(
                *[
                    {
                        **entry.data,
                        "slug": entry.slug,
                        "entry-date": entry.entry_date.isoformat(),
                    }
                    for entry in self.entries
                    if entry.entry_date <= snapshot_date
                ]
            )
        )

    def change_history(self):
        last_entry = None
        result = []
        for entry in self.entries:
            if last_entry and entry.data == last_entry.data:
                last_entry = entry
                continue
            result.append(entry)
            last_entry = entry
        return result
