import re
import logging

import polars as pl

from .phase import PolarsPhase

normalise_pattern = re.compile(r"[^a-z0-9-]")


def normalise(value):
    return re.sub(normalise_pattern, "", value.lower())


def key(entry_number="", prefix="", reference="", organisation=""):
    entry_number = str(entry_number)
    prefix = normalise(prefix)
    reference = normalise(reference)
    organisation = normalise(organisation)
    return ",".join([entry_number, prefix, reference, organisation])


class EntityLookupPhase(PolarsPhase):
    """
    Look up entity numbers by CURIE (prefix:reference).
    """

    def __init__(
        self,
        lookups=None,
        redirect_lookups=None,
        issue_log=None,
        operational_issue_log=None,
        entity_range=None,
    ):
        if lookups is None:
            lookups = {}
        if redirect_lookups is None:
            redirect_lookups = {}
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.issues = issue_log
        self.operational_issues = operational_issue_log
        self.entity_range = entity_range or []

    def _lookup(self, prefix="", reference="", organisation="", entry_number=""):
        return (
            self.lookups.get(
                key(prefix=prefix, entry_number=entry_number), ""
            )
            or self.lookups.get(
                key(prefix=prefix, organisation=organisation, reference=reference), ""
            )
            or self.lookups.get(
                key(prefix=prefix, reference=reference), ""
            )
        )

    def _redirect(self, entity):
        if self.redirect_lookups and entity:
            redirect_entry = self.redirect_lookups.get(str(entity), "")
            if redirect_entry:
                if redirect_entry["status"] == "301":
                    return redirect_entry["entity"]
                elif redirect_entry["status"] == "410":
                    return ""
        return entity

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "entity" not in df.columns:
            df = df.with_columns(pl.lit("").alias("entity"))
        if "prefix" not in df.columns:
            df = df.with_columns(pl.lit("").alias("prefix"))
        if "reference" not in df.columns:
            df = df.with_columns(pl.lit("").alias("reference"))

        entities = []
        for row in df.iter_rows(named=True):
            existing = row.get("entity", "") or ""
            prefix = row.get("prefix", "") or ""
            reference = row.get("reference", "") or ""
            organisation = (row.get("organisation", "") or "").replace(
                "local-authority-eng", "local-authority"
            )
            entry_number = row.get("__entry_number", "")
            line_number = row.get("__line_number", "")
            resource = row.get("__resource", "")

            if existing:
                entities.append(existing)
                continue

            if not prefix:
                entities.append("")
                continue

            entity = self._lookup(
                prefix=prefix,
                reference=reference,
                organisation=organisation,
                entry_number=entry_number,
            )

            if entity and self.entity_range:
                try:
                    if int(entity) not in range(
                        int(self.entity_range[0]), int(self.entity_range[1])
                    ):
                        if self.issues:
                            self.issues.resource = resource
                            self.issues.line_number = line_number
                            self.issues.entry_number = entry_number
                            self.issues.log_issue(
                                "entity", "entity number out of range", entity
                            )
                except (ValueError, TypeError):
                    pass

            if not entity:
                curie = f"{prefix}:{reference}"
                if self.issues:
                    self.issues.resource = resource
                    self.issues.line_number = line_number
                    self.issues.entry_number = entry_number
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
                entities.append("")
            else:
                entity = self._redirect(entity)
                entities.append(entity)

        df = df.with_columns(pl.Series("entity", entities))

        # Record entity map for issue log
        if self.issues:
            for row in df.iter_rows(named=True):
                entry_number = row.get("__entry_number", "")
                entity = row.get("entity", "")
                if entity:
                    self.issues.record_entity_map(entry_number, entity)

        return df


class FactLookupPhase(PolarsPhase):
    """
    Look up reference-entity for facts.
    """

    def __init__(
        self,
        lookups=None,
        redirect_lookups=None,
        issue_log=None,
        odp_collections=None,
    ):
        if lookups is None:
            lookups = {}
        if redirect_lookups is None:
            redirect_lookups = {}
        if odp_collections is None:
            odp_collections = []
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.issues = issue_log
        self.odp_collections = odp_collections

    def _lookup(self, prefix="", reference="", organisation=""):
        return (
            self.lookups.get(
                key(prefix=prefix, organisation=organisation, reference=reference), ""
            )
            or self.lookups.get(
                key(prefix=prefix, reference=reference), ""
            )
        )

    def _check_associated_organisation(self, entity):
        reverse_lookups = {}
        for k, v in self.lookups.items():
            if v not in reverse_lookups:
                reverse_lookups[v] = []
            reverse_lookups[v].append(k)

        if entity in reverse_lookups:
            keywords = {"authority", "development", "government"}
            for k in reverse_lookups[entity]:
                parts = k.split(",")
                if len(parts) > 3 and any(kw in parts[3] for kw in keywords):
                    return ""
        return entity

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        if "reference-entity" not in df.columns:
            df = df.with_columns(pl.lit("").alias("reference-entity"))

        ref_entities = []
        for row in df.iter_rows(named=True):
            prefix = row.get("prefix", "") or ""
            reference = row.get("reference", "") or ""
            entity_number = row.get("entity", "") or ""
            line_number = row.get("line-number", "") or row.get("__line_number", "")
            organisation = (row.get("organisation", "") or "").replace(
                "local-authority-eng", "local-authority"
            )

            if not (prefix and reference and entity_number):
                ref_entities.append(row.get("reference-entity", "") or "")
                continue

            find_entity = self._lookup(
                prefix=prefix, organisation=organisation, reference=reference
            )
            if not find_entity:
                find_entity = self._lookup(prefix=prefix, reference=reference)
                find_entity = self._check_associated_organisation(find_entity)

            if not find_entity or (
                str(find_entity) in self.redirect_lookups
                and int(self.redirect_lookups[str(find_entity)].get("status", 0)) == 410
            ):
                if self.odp_collections and prefix in self.odp_collections:
                    if self.issues:
                        self.issues.log_issue(
                            prefix,
                            "missing associated entity",
                            reference,
                            line_number=line_number,
                        )
                ref_entities.append("")
            else:
                ref_entities.append(str(find_entity))

        df = df.with_columns(pl.Series("reference-entity", ref_entities))
        return df


class PrintLookupPhase(PolarsPhase):
    """
    Print new lookup entries for unresolved entities.
    """

    def __init__(self, lookups=None, redirect_lookups=None):
        if lookups is None:
            lookups = {}
        if redirect_lookups is None:
            redirect_lookups = {}
        self.lookups = lookups
        self.redirect_lookups = redirect_lookups
        self.new_lookup_entries = []

    def _lookup(self, prefix="", reference="", organisation="", entry_number=""):
        return (
            self.lookups.get(
                key(prefix=prefix, entry_number=entry_number), ""
            )
            or self.lookups.get(
                key(prefix=prefix, organisation=organisation, reference=reference), ""
            )
            or self.lookups.get(
                key(prefix=prefix, reference=reference), ""
            )
        )

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        if df is None or df.height == 0:
            return df

        for row in df.iter_rows(named=True):
            prefix = row.get("prefix", "") or ""
            organisation = row.get("organisation", "") or ""
            reference = row.get("reference", "") or ""
            entry_number = row.get("__entry_number", "")

            entity = ""
            if prefix:
                entity = self._lookup(
                    prefix=prefix,
                    reference=reference,
                    organisation=organisation,
                    entry_number=entry_number,
                )

            if not entity:
                if prefix and organisation and reference:
                    if "," in reference:
                        reference = f'"{reference}"'
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
                        + row.get("__resource", "")
                    )

        return df
