import hashlib
from .phase import Phase


def fact_hash(entity, field, value):
    data = entity + ":" + field + ":" + value
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class FactorPhase(Phase):
    """
    add fact identifiers, filter out facts with a missing entity
    """

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]

            if row["entity"]:
                row["fact"] = fact_hash(row["entity"], row["field"], row["value"])

            yield stream_data
