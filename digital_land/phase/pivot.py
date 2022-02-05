from .phase import Phase


class PivotPhase(Phase):
    """
    split entries into a series of facts
    """

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            for field, value in sorted(row.items()):

                if field in ["entity"]:
                    continue

                stream_data["row"] = {
                    # fact
                    "fact": "",
                    "entity": row.get("entity", ""),
                    "field": field,
                    "value": value,
                    # entry
                    "resource": stream_data["resource"],
                    "line-number": stream_data["line-number"],
                    "entry-date": row["entry-date"],
                }

                yield stream_data
