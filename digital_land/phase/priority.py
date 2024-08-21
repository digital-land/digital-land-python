import sqlite3
from .phase import Phase


class PriorityPhase(Phase):
    """
    Deduce priority of the entry when assembling facts
    """

    def __init__(self, connection=None):
        if not connection:
            connection = sqlite3.connect("var/cache/pipeline.sqlite3")
        self.cursor = connection.cursor()

    def entity_organisation(self, entity):
        self.cursor.execute(
            f"select organisation from entity_organisation where entity_minimum <= {entity} and entity_maximum >= {entity}"
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    def priority(self, entity, organisation):
        return 1 if self.entity_organisation(entity) == organisation else 2

    def process(self, stream):
        for block in stream:
            row = block["row"]
            block["priority"] = self.priority(row["entity"], row["organisation"])
            yield block
