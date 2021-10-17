import re

# Lookup entities

class Lookup:
    def __init__(self, lookups={}, key_field=""):
        self.lookups = lookups
        self.key_field = key_field

    # TBD: use same method as pipeline normalise
    normalise_pattern = re.compile(r"[^a-z0-9-]")

    def normalise(self, value):
        return re.sub(self.normalise_pattern, "", value.lower())

    def lookup_entity(self, resource, organisation, value):
        value = self.normalise(value)
        return self.lookups.get(organisation + value, "") or self.lookups.get(value, "")

    def lookup(self, reader):
        for stream_data in reader:
            row = stream_data["row"]

            if not row.get("entity", ""):
                if self.key_field:
                    row["entity"] = self.lookup_entity(
                        stream_data["resource"],
                        row.get("organisation", ""),
                        row.get(self.key_field, ""),
                    )

            yield stream_data
