from .phase import Phase


class OrganisationPhase(Phase):
    """
    lookup the organisation
    """

    def __init__(self, organisation={}, issues=None):
        self.organisation = organisation
        self.issues = issues

    def process(self, stream):
        for block in stream:
            row = block["row"]
            if self.issues:
                self.issues.resource = block["resource"]
                self.issues.line_number = block["line-number"]
                self.issues.entry_number = block["entry-number"]

            organisation_value = row.get("organisation", "")
            if organisation_value:
                row["organisation"] = self.organisation.lookup(organisation_value)
            else:
                row["organisation"] = ""

            # Only report invalid organisations when a value was supplied.
            if organisation_value and not row.get("organisation", "") and self.issues:
                self.issues.log_issue(
                    "organisation", "invalid organisation", organisation_value
                )

            # Store at block level for post-pivot lookups
            block["organisation"] = row["organisation"]

            yield block
