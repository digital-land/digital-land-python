from .phase import Phase


class EntityPrefixPhase(Phase):
    """
    ensure an entry has a prefix field
    """

    def __init__(self, dataset=None):
        self.dataset = dataset

    def process(self, stream):
        for block in stream:
            row = block["row"]

            if not row.get("prefix", ""):
                row["prefix"] = self.dataset

            yield block
