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

    def __init__(
        self,
        lookups={},
        redirect_lookups={},
        issue_log=None,
        operational_issue_log=None,
        entity_range=[],
        lookup_rules=None,
    ):
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.issues = issue_log
        self.operational_issues = operational_issue_log
        self.reverse_lookups = self.build_reverse_lookups()
        self.entity_range = entity_range
        self.lookup_rules = lookup_rules or []

    def build_reverse_lookups(self):
        reverse_lookups = {}
        for key, value in self.lookups.items():
            if value not in reverse_lookups:
                reverse_lookups[value] = []
            reverse_lookups[value].append(key)
        return reverse_lookups

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def lookup_rule(self, prefix="", organisation="", reference=""):
        try:
            ref_int = int(reference)
        except (ValueError, TypeError):
            return ""

        prefix_norm = normalise(prefix)
        org_norm = normalise(organisation)

        for rule in self.lookup_rules:
            if normalise(rule["prefix"]) != prefix_norm:
                continue
            rule_org = normalise(rule.get("organisation", ""))
            if rule_org and rule_org != org_norm:
                continue
            candidate = ref_int + rule["offset"]
            if rule["entity-minimum"] <= candidate <= rule["entity-maximum"]:
                return str(candidate)

        return ""

    def check_associated_organisation(self, entity):
        if entity in self.reverse_lookups:
            keywords = {"authority", "development", "government"}
            for key in self.reverse_lookups[entity]:
                parts = key.split(",")
                if len(parts) > 3 and any(keyword in parts[3] for keyword in keywords):
                    return ""
        return entity

    def get_entity(self, block, organisation=None):
        """Look up the entity for a block's prefix and reference.

        Args:
            organisation (str, optional): Organisation to resolve the entity against.
                Defaults to None, meaning use the organisation already on the row.
                EntityLookupPhase passes this explicitly when fanning out a
                multi-organisation resource, where the row's organisation is blank.

        Returns:
            str: The entity number, or "" if nothing matched.
        """
        row = block["row"]
        prefix = row.get("prefix", "")
        reference = row.get("reference", "")
        # Eventually won't be needed but leave in for now
        if organisation is None:
            organisation = row.get("organisation", "")
        organisation = organisation.replace("local-authority-eng", "local-authority")
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
            or self.lookup(prefix=prefix, reference=reference)
            or self.lookup_rule(
                prefix=prefix,
                organisation=organisation,
                reference=reference,
            )
        )

        if entity and self.entity_range:
            if (
                int(entity)
                not in range(int(self.entity_range[0]), int(self.entity_range[1]))
                and self.issues
            ):
                self.issues.log_issue(
                    "entity",
                    "entity number out of range",
                    entity,
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

    def _log_unknown_entity(self, reference, curie, line_number):
        """Raise an unknown-entity issue, distinguishing a missing reference."""
        if not self.issues:
            return
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
            if self.operational_issues:
                self.operational_issues.log_issue(
                    "entity",
                    "unknown entity",
                    curie,
                    line_number=line_number,
                )

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
                                if self.operational_issues:
                                    self.operational_issues.log_issue(
                                        "entity",
                                        "unknown entity",
                                        curie,
                                        line_number=line_number,
                                    )
                    else:
                        row[self.entity_field] = self.redirect_entity(
                            row[self.entity_field]
                        )
            yield block


class EntityLookupPhase(LookupPhase):
    """
    lookup the entity for an entry's reference

    A resource is identified by the hash of its contents, so when several
    organisations publish identical data the collection merges them into one
    resource carrying every organisation's code (e.g. a joint local plan, or a
    brownfield register published by two authorities). Those rows arrive here
    with a blank organisation, because the resource-level default is only
    applied when the resource has exactly one organisation.

    When given more than one organisation this phase resolves the entity once
    per organisation and emits a row per distinct entity. With one organisation
    (or none) it behaves exactly as it always has.
    """

    entity_field = "entity"

    def __init__(
        self,
        lookups={},
        redirect_lookups={},
        issue_log=None,
        operational_issue_log=None,
        entity_range=[],
        lookup_rules=None,
        organisations=None,
    ):
        """
        Args:
            organisations (list, optional): Organisation codes associated with the
                resource. More than one means the resource is shared, and the entity
                is resolved once per organisation. Defaults to None (no fan-out).
        """

        super().__init__(
            lookups=lookups,
            redirect_lookups=redirect_lookups,
            issue_log=issue_log,
            operational_issue_log=operational_issue_log,
            entity_range=entity_range,
            lookup_rules=lookup_rules,
        )
        self.organisations = organisations or []

    def process(self, stream):
        # A resource may be associated with more than one organisation (e.g. a
        # joint local plan shared by two authorities). In that case the entity
        # is resolved once per organisation and a row is emitted per distinct
        # entity. With a single organisation (or none) each row resolves to at
        # most one entity.
        if len(self.organisations) > 1:
            yield from self._process_multi_org(stream)
            return

        for block in super().process(stream):
            if self.issues:
                self.issues.record_entity_map(
                    block["entry-number"], block["row"]["entity"]
                )
            yield block

    def _process_multi_org(self, stream):
        """Resolve the entity once per organisation and fan out the row.

        For each entry the reference is looked up against every organisation on
        the resource. Distinct entities produce distinct rows, each stamped with
        the organisation it was looked up with; if two organisations resolve to
        the same entity only one row is emitted. An organisation that resolves
        nothing raises an unknown-entity issue and produces no row, so an entry
        may yield fewer rows than there are organisations — including none.

        The organisation is set on the block as well as the row because
        FactLookupPhase reads it after the pivot, once row-level fields are gone.
        """

        for block in stream:
            row = block["row"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            curie = f"{prefix}:{reference}"
            line_number = block["line-number"]

            # Nothing to look up if there is no prefix, or an entity has
            # already been assigned upstream.
            if not prefix or row.get(self.entity_field, ""):
                if self.issues:
                    self.issues.record_entity_map(
                        block["entry-number"], row.get(self.entity_field, "")
                    )
                yield block
                continue

            emitted_entities = set()
            for organisation in self.organisations:
                entity = self.get_entity(block, organisation=organisation)

                if not entity:
                    # no entity for this organisation: log an unknown-entity
                    # issue and emit no row for it
                    self._log_unknown_entity(reference, curie, line_number)
                    continue

                entity = self.redirect_entity(entity)
                if not entity or entity in emitted_entities:
                    continue
                emitted_entities.add(entity)

                new_block = dict(block)
                new_block["row"] = dict(row)
                new_block["row"][self.entity_field] = entity
                new_block["row"]["organisation"] = organisation
                new_block["organisation"] = organisation
                if self.issues:
                    self.issues.record_entity_map(block["entry-number"], entity)
                yield new_block


class FactLookupPhase(LookupPhase):
    def __init__(
        self,
        lookups={},
        redirect_lookups={},
        issue_log=None,
        odp_collections=None,
        package_prefixes=None,
        lookup_rules=None,
    ):
        super().__init__(
            lookups, redirect_lookups, issue_log, lookup_rules=lookup_rules
        )
        self.entity_field = "reference-entity"
        self.odp_collections = odp_collections
        self.package_prefixes = package_prefixes or {}

    def process(self, stream):
        for block in stream:
            row = block["row"]
            line_number = row.get("line-number", "")
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            entity_number = row.get("entity", "")

            if not (prefix and reference and entity_number):
                yield block
                continue

            # Get organisation from block metadata (set by OrganisationPhase)
            organisation = block.get("organisation", "").replace(
                "local-authority-eng", "local-authority"
            )

            find_entity = self.lookup(
                prefix=prefix,
                organisation=organisation,
                reference=reference,
            )
            if not find_entity:
                # TBD this needs to specifically not match unless the organisation and other columns
                # are empty in the lookups.csv probably isn't a change here.
                # or by the CURIE
                find_entity = self.lookup(prefix=prefix, reference=reference)

                # When obtaining an entity number using only the prefix and reference, check if the
                # lookup includes an associated organisation. If it does, do not use the entity number,
                # as it is organisation specific.
                find_entity = self.check_associated_organisation(find_entity)

            for package_prefix in self.package_prefixes.get(prefix, []):
                if find_entity:
                    break

                find_entity = self.lookup(
                    prefix=package_prefix,
                    organisation=organisation,
                    reference=reference,
                )
                if not find_entity:
                    find_entity = self.lookup(
                        prefix=package_prefix, reference=reference
                    )
                    find_entity = self.check_associated_organisation(find_entity)

            if not find_entity:
                find_entity = self.lookup_rule(
                    prefix=prefix,
                    organisation=organisation,
                    reference=reference,
                )

            if not find_entity or (
                str(find_entity) in self.redirect_lookups
                and int(self.redirect_lookups[str(find_entity)].get("status", 0)) == 410
            ):
                if (self.odp_collections and prefix in self.odp_collections) or (
                    prefix in self.package_prefixes
                ):
                    self.issues.log_issue(
                        prefix,
                        "missing associated entity",
                        reference,
                        line_number=line_number,
                    )
            else:
                row[self.entity_field] = find_entity
            yield block


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
                    logging.info(
                        "No reference found for entry: "
                        + str(entry_number)
                        + " in resource: "
                        + block["resource"]
                    )
        yield
