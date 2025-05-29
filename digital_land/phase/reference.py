import re
import logging
import warnings
from .phase import Phase

logger = logging.getLogger(__name__)


# match CURIEs which don't have a space after the colon
curie_re = re.compile(r"(?P<prefix>[A-Za-z0-9_-]+):(?P<reference>[A-Za-z0-9_-].*)$")


def split_curie(value):
    match = curie_re.match(value)
    if not match:
        return ["", value]

    return [match.group("prefix"), match.group("reference")]


class EntityReferencePhase(Phase):
    """
    ensure an entry has the prefix and reference fields
    """

    def __init__(self, dataset=None, prefix=None, specification=None, issues=None):
        self.dataset = dataset
        self.specification = specification
        if prefix:
            self.prefix = prefix
        elif specification:
            warnings.warn(
                "using specification is depreciated, provide prefix arguement instead",
                DeprecationWarning,
                2,
            )
            self.prefix = specification.dataset_prefix(self.dataset)
        else:
            self.prefix = dataset
        self.issues = issues

    def process_row(self, row):
        reference = row.get("reference", "") or row.get(self.dataset, "")
        reference_prefix, reference = split_curie(reference)

        if self.issues and reference_prefix:
            self.issues.log_issue(
                "reference",
                "reference value contains reference_prefix",
                reference_prefix,
            )

        # crude fix for (hopefully) one-of issue with Newham Council
        # if this type of problem becomes common, a more scalable
        # solution will need to be sought.
        if "UPRN" in reference_prefix:
            reference_prefix = ""

        prefix = row.get("prefix", "") or reference_prefix or self.prefix
        return prefix, reference

    def process(self, stream):
        for block in stream:
            row = block["row"]
            (row["prefix"], row["reference"]) = self.process_row(row)
            yield block


class FactReferencePhase(EntityReferencePhase):
    """
    ensure a fact which is a reference has a prefix and reference field
    """

    def __init__(
        self,
        field_typology_map=None,
        field_prefix_map=None,
        dataset=None,
        specification=None,
    ):
        if dataset:
            warnings.warn(
                "depreciated dataset arguement is not required", DeprecationWarning, 2
            )
        self.specification = specification
        self.field_typology_map = field_typology_map
        self.field_prefix_map = field_prefix_map

    def get_field_typology_name(self, field_name):
        if self.field_typology_map:
            try:
                typology = self.field_typology_map[field_name]
            except KeyError:
                raise ValueError(f"field {field_name} is not in field_typology_map")
        elif self.specification:
            warnings.warn(
                "using specification is depreciated, provide field_typology_map arguement instead",
                DeprecationWarning,
                2,
            )
            typology = self.specification.field_typology(field_name)
        else:
            raise ValueError("please provide field_typology_map")

        return typology

    def get_field_prefix(self, field_name):
        if self.field_prefix_map:
            try:
                prefix = self.field_prefix_map[field_name]
            except KeyError:
                raise ValueError(f"field {field_name} is not in field_prefix_map")
        elif self.specification:
            warnings.warn(
                "using specification is depreciated, provide field_prefix_map arguement instead",
                DeprecationWarning,
                2,
            )
            prefix = self.specification.field_prefix(field_name)
        else:
            return field_name

        return prefix

    def process_row(self, row):
        prefix = row.get("prefix", "")
        reference = row.get("reference", "")

        if prefix and reference:
            return prefix, reference

        field = row["field"]
        typology = self.get_field_typology_name(field)

        # TBD: infer if a field is a reference from the specification
        # could use field:datatype map to specificy reference and use that
        # do not pull reference into a pipeline phase only get info out of it for this
        if typology in [
            "category",
            "document",
            "geography",
            "organisation",
            "policy",
            "legal-instrument",
        ]:
            value_prefix, value_reference = split_curie(row["value"])
            prefix = prefix or value_prefix or self.get_field_prefix(field)
            reference = reference or value_reference

        return prefix, reference
