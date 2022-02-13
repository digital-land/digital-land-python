from .phase import Phase


class MigratePhase(Phase):
    """
    change field names to match the latest specification
    """

    def __init__(self, fields, migrations, organisation={}):
        self.migrations = migrations
        self.fields = list(set(fields + ["entity", "organisation", "prefix", "reference"]))

    def process(self, stream):
        for block in stream:
            row = block["row"]
            o = {}

            for field in self.fields:
                o[field] = row.get(self.migrations.get(field, ""), row.get(field, ""))

            # TBD: move to separate point phase
            if set(["GeoX", "GeoY"]).issubset(row.keys()) and "point" in self.fields:
                if row["GeoX"] and row["GeoY"]:
                    o["point"] = f"POINT({row['GeoX']} {row['GeoY']})"

            block["row"] = o
            yield block
