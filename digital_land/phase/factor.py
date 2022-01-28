#!/usr/bin/env python3

#
#  split entries into a series of facts
#

import hashlib
from .phase import Phase


def fact_hash(entity, field, value):
    data = entity + ":" + field + ":" + value
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class FactorPhase(Phase):
    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            for field in self.fields:
                entity = row["entity"]
                value = row["value"]
                fact = fact_hash(entity, field, value)
                o = {
                    "fact": fact,
                    "entity": entity,
                    "field": field,
                    "value": value,
                    "start-date": row["start-date"],
                    "end-date": row["end-date"],
                    "entry-date": row["entry-date"],
                    "resource": stream_data["resource"],
                    "row-number": stream_data["row-number"],
                }

                stream_data["row"] = o
                yield stream_data
