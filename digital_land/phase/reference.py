from .phase import Phase


class ReferencePhase(Phase):
    """
    ensure an entry has the prefix and reference CURIE fields
    """

    def __init__(self, specification):
        self.specification = specification

    @staticmethod
    def split_curie(value):
        s = value.split(":", 2)
        if len(s) == 2:
            return s
        return ["", value]

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            dataset = stream_data["dataset"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")

            if not prefix or not reference:
                self.process_row(row, dataset)

            yield stream_data


class EntityReferencePhase(ReferencePhase):
    def process_row(self, row, dataset):

        reference = row.get("reference", "")

        if not reference:
            reference = row.get(self.specification.key_field(dataset), "")

        reference_prefix, reference = self.split_curie(reference)
        row["prefix"] = (
            row.get("prefix", "")
            or reference_prefix
            or self.specification.dataset_prefix(dataset)
        )
        row["reference"] = reference


class FactReferencePhase(ReferencePhase):
    def process_row(self, row, dataset):
        field = row["field"]
        typology = self.specification.field[field]["typology"]

        # TBD: infer if a field is a reference from the specification
        if typology in [
            "category",
            "document",
            "geography",
            "organisation",
            "policy",
            "legal-instrument",
        ]:
            value_prefix, value_reference = self.split_curie(row["value"])
            row["prefix"] = (
                row.get("prefix", "")
                or value_prefix
                or self.specification.field_prefix(field)
            )
            row["reference"] = row.get("reference", "") or value_reference
