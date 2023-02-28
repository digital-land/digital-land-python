import re
from .phase import Phase


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

    def __init__(self, dataset=None, specification=None):
        self.dataset = dataset
        self.specification = specification
        if specification:
            self.prefix = specification.dataset_prefix(self.dataset)
        else:
            self.prefix = dataset

    def process_row(self, row):
        reference = row.get("reference", "") or row.get(self.dataset, "")
        reference_prefix, reference = split_curie(reference)

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

    def process_row(self, row):
        prefix = row.get("prefix", "")
        reference = row.get("reference", "")

        if prefix and reference:
            return prefix, reference

        # TBD: infer if a field is a reference from the specification
        field = row["field"]
        typology = self.specification.field_typology(field)
        if typology in [
            "category",
            "document",
            "geography",
            "organisation",
            "policy",
            "legal-instrument",
        ]:
            value_prefix, value_reference = split_curie(row["value"])
            prefix = prefix or value_prefix or self.specification.field_prefix(field)
            reference = reference or value_reference

        return prefix, reference
