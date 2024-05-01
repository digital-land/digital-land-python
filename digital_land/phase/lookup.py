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

    entity_field = None

    def __init__(self, lookups={}, redirect_lookups={}, issue_log=None):
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.issues = issue_log

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def get_entity(self, block):
        row = block["row"]
        prefix = row.get("prefix", "")
        reference = row.get("reference", "")
        # Eventually won't be needed but leave in for now
        organisation = row.get("organisation", "").replace(
            "local-authority-eng", "local-authority"
        )
        entry_number = block["entry-number"]

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
            # TBD this needs to specifically not match unless the organisation and other columns
            # are empty in the lookups.csv probably isn't a change here.
            # or by the CURIE
            or self.lookup(prefix=prefix, reference=reference)
        )

        return entity

    def redirect_entity(self, entity):
        """
        Given an entity number can check the redirect lookups to see if the entity
        has been removed or redirected.
        """
        if self.redirect_lookups:
            redirect_entity = self.redirect_lookups.get(entity, "")
            if redirect_entity:
                if redirect_entity["status"] == "301":
                    return redirect_entity["entity"]
                elif redirect_entity["status"] == "410":
                    return ""
        return entity

    def process(self, stream):
        for block in stream:
            row = block["row"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            curie = f"{prefix}:{reference}"
            line_number = block["line-number"]

            # TODO do we need to check for prefix here?
            if prefix:
                if not row.get(self.entity_field, ""):
                    row[self.entity_field] = self.get_entity(block)

                    if not row[self.entity_field]:
                        if self.issues:
                            if not reference:
                                self.issues.log_issue(
                                    "entity",
                                    "unknown entity - missing reference",
                                    curie,
                                    line_number=line_number,
                                )
                            else:
                                self.issues.log_issue(
                                    "entity",
                                    "unknown entity",
                                    curie,
                                    line_number=line_number,
                                )
                        yield block
                    else:
                        row[self.entity_field] = self.redirect_entity(
                            row[self.entity_field]
                        )
                        yield block


class EntityLookupPhase(LookupPhase):
    entity_field = "entity"


class FactLookupPhase(LookupPhase):
    entity_field = "reference-entity"


class PrintLookupPhase(LookupPhase):
    def __init__(self, lookups={}, redirect_lookups={}):
        super().__init__(lookups, redirect_lookups)
        self.entity_field = "entity"
        self.new_lookup_entries = []

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            organisation = row.get("organisation", "")
            reference = row.get("reference", "")
            if "," in reference:
                reference = f'"{reference}"'

            if prefix:
                if not row.get(self.entity_field, ""):
                    row[self.entity_field] = self.get_entity(block)

            if not row[self.entity_field]:
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
        yield
