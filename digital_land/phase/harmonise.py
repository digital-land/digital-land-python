from .phase import Phase


class HarmonisePhase(Phase):
    """
    harmonise field values to match their datatype
    """

    def __init__(self, specification, issues):
        self.specification = specification
        self.issues = issues

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        self.issues.fieldname = fieldname

        datatype = self.specification.field_type(fieldname)
        return datatype.normalise(value, issues=self.issues)

    def process(self, stream):
        for block in stream:
            row = block["row"]

            for field in row:
                row[field] = self.harmonise_field(field, row[field])

            # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs with a wikipedia-en prefix
            if row.get("wikipedia", "").startswith("http"):
                self.issues.log_issue("wikipedia", "removed prefix", row["wikipedia"])
                row["wikipedia"] = row["wikipedia"].replace(
                    "https://en.wikipedia.org/wiki/", ""
                )

            yield block
