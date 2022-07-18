import hashlib
from .phase import Phase


def fact_hash(entity, field, value):
    data = entity + ":" + field + ":" + value
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class FactorPhase(Phase):
    """
    add a fact hash identifier
    """

    def process(self, stream):
        for block in stream:
            row = block["row"]

            if row["entity"]:
                row["fact"] = fact_hash(row["entity"], row["field"], row["value"])

            yield block
