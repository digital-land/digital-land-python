import re

from .phase import Phase


# TBD: use same method as piperow normalise
normalise_pattern = re.compile(r"[^a-z0-9-]")


def normalise(value):
    return re.sub(normalise_pattern, "", value.lower())


def key(entry_number="", prefix="", reference="", organisation=""):
    entry_number = str(entry_number)
    prefix = normalise(prefix)
    reference = normalise(reference)
    organisation = normalise(organisation)
    return ",".join([entry_number, prefix, reference, organisation])


class LookupPhase(Phase):
    """
    lookup entity numbers by CURIE
    """

    def __init__(self, lookups={},redirect_lookups={}):
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            organisation = row.get("organisation", "")

            if prefix:
                if not row.get(self.entity_field, ""):
                    row[self.entity_field] = (
                        # by the resource and row number
                        (
                            self.entity_field == "entity"
                            and self.lookup(prefix=prefix, entry_number=entry_number)
                        )
                        # TBD: fixup prefixes so this isn't needed ..
                        # or by the organisation and the reference
                        or self.lookup(
                            prefix=prefix,
                            organisation=organisation,
                            reference=reference,
                        )
                        # or by the CURIE
                        or self.lookup(prefix=prefix, reference=reference)
                    )
                    if self.redirect_lookups
                
            

            yield block


class EntityLookupPhase(LookupPhase):
    entity_field = "entity"


class FactLookupPhase(LookupPhase):
    entity_field = "reference-entity"
