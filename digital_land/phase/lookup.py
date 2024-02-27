import re
import logging

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

    def __init__(self, entity_field):
        self.entity_field = entity_field

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            organisation = row.get("organisation", "")
            organisation = organisation.replace(
                "local-authority-eng", "local-authority"
            )

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

            yield block


class EntityLookupPhase(LookupPhase):
    def __init__(self, **kwargs):
        super().__init__(entity_field="entity", **kwargs)


class FactLookupPhase(LookupPhase):
    def __init__(self, **kwargs):
        super().__init__(entity_field="reference-entity", **kwargs)


class PrintLookupPhase(Phase):
    def __init__(self, lookups={}, redirect_lookups={}):
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.entity_field = "entity"
        self.new_lookup_entries = []

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            if "," in reference:
                reference = f'"{reference}"'
            organisation = row.get("organisation", "")
            if prefix:
                if not row.get(self.entity_field, ""):
                    entity = (
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
                    )
                    if self.redirect_lookups:
                        old_entity = row[self.entity_field]
                        new_entity = self.redirect_lookups.get(old_entity, "")
                        if new_entity and new_entity["status"] == "301":
                            row[self.entity_field] = new_entity["entity"]
                        elif new_entity and new_entity["status"] == "410":
                            row[self.entity_field] = ""
            if not entity:
                if prefix and organisation and reference:
                    new_lookup = {
                        "prefix": prefix,
                        "organisation": organisation,
                        "reference": reference,
                    }
                    self.new_lookup_entries.append([new_lookup])
                elif not reference:
                    logging.error(
                        "No reference found for entry: "
                        + str(entry_number)
                        + " in resource: "
                        + block["resource"]
                    )
            yield block
