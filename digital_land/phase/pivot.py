from .phase import Phase


class PivotPhase(Phase):
    """
    split entries into a series of facts
    """

    def process(self, stream):
        for block in stream:
            row = block["row"]
            for field, value in sorted(row.items()):
                if field in ["entity"]:
                    continue

                block["row"] = {
                    # fact
                    "fact": "",
                    "entity": row.get("entity", ""),
                    "field": field,
                    "value": value,
                    # entry
                    "priority": block["priority"],
                    "resource": block["resource"],
                    "line-number": block["line-number"],
                    "entry-number": block["entry-number"],
                    "entry-date": row["entry-date"],
                }

                yield block
