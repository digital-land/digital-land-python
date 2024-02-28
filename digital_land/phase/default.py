from .phase import Phase


class DefaultPhase(Phase):
    def __init__(
        self,
        issues=None,
        default_fields={},
        default_values={},
    ):
        self.issues = issues
        self.default_values = default_values
        self.default_fields = default_fields

    def process(self, stream):
        for block in stream:
            row = block["row"]

            # TBD: change log_issue to take these values from the block
            if self.issues:
                self.issues.resource = block["resource"]
                self.issues.line_number = block["line-number"]
                self.issues.entry_number = block["entry-number"]

            # default field from another field
            for field in self.default_fields:
                if not row.get(field, ""):
                    default_field = self.default_fields.get(field, "")
                    value = row.get(default_field, "")
                    if value:
                        self.issues.log_issue(field, "default-field", default_field)
                        row[field] = value

            # default field value
            for field in self.default_values:
                if not row.get(field, ""):
                    value = self.default_values.get(field, "")
                    if value:
                        # TODO organisation and entry-date are being replaced systematically
                        # using default-value. This is cuasing tons of issues which are meaningless
                        # need to improve default-field to be able to map this
                        if field not in ["organisation", "entry-date"]:
                            self.issues.log_issue(field, "default-value", value)
                        row[field] = value

            yield block
