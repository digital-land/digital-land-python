import re

from .phase import Phase


# TBD: use same method as pipeline normalise
normalise_pattern = re.compile(r"[^a-z0-9-]")


def normalise(value):
    return re.sub(normalise_pattern, "", value.lower())


def key(line_number="", organisation="", prefix="", reference=""):
    prefix = normalise(prefix)
    reference = normalise(reference)
    line_number = str(line_number)
    return ",".join([line_number, organisation, prefix, reference])


class LookupPhase(Phase):
    """
    lookup entity numbers by CURIE
    """

    def __init__(self, lookups, field="", entity_field=""):
        self.lookups = lookups

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            line_number = block["line-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            organisation = row.get("organisation", "")

            if prefix:
                if not row.get(self.entity_field, ""):
                    row[self.entity_field] = (
                        self.lookup(line_number=line_number, prefix=prefix)
                        or self.lookup(
                            organisation=organisation,
                            prefix=prefix,
                            reference=reference,
                        )
                        or self.lookup(prefix=prefix, reference=reference)
                    )

            yield block


class EntityLookupPhase(LookupPhase):
    entity_field = "entity"


class FactLookupPhase(LookupPhase):
    entity_field = "reference-entity"
