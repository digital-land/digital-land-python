from .phase import Phase


def split_curie(value):
    s = value.split(":", 2)
    if len(s) == 2:
        return s
    return ["", value]


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
