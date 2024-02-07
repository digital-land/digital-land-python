from .dataset import DatasetCheckpoint
import functools

class EntityChecksCheckpoint(DatasetCheckpoint):
    def __init__(self, *args):
        super().__init__(*args)

    def load(self):
        self.expectations = [
            functools.partial(EntityChecksCheckpoint.check_old_entities, self),
            functools.partial(EntityChecksCheckpoint.check_entities_without_doc_url, self),
        ]

    def check_old_entities(self):
        row = self.connection.execute(
            "SELECT COUNT(*) FROM entity WHERE entity IN (SELECT entity FROM old_entity)"
            ).fetchone()

        count = row[0]
        if count == 0:
            return (True, f"No entities in old-entities", [])
        else:
            return (False, f"{count} entities in old-entities", [])


    def check_entities_without_doc_url(self):
        row = self.connection.execute(
            "SELECT COUNT(*) FROM entity WHERE NOT EXISTS "
             "(SELECT * FROM fact WHERE field='documentation-url' and entity=entity.entity)"
            ).fetchone()

        count = row[0]
        if count == 0:
            return (True, f"No entities without documentation-url fact", [])
        else:
            return (False, f"{count} entities hae no documentation-url fact", [])

