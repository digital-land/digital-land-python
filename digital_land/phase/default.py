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
                self.issues.dataset = block["dataset"]
                self.issues.resource = block["resource"]
                self.issues.line_number = block["line-number"]
                self.issues.entry_number = block["entry-number"]

            # default is from the contents of another field in this row
            for field in self.default_fields:
                for default_field in self.default_fields[field]:
                    value = row.get(default_field, "")
                    if value and not row.get(field, ""):
                        self.issues.log_issue(field, "default-field", default_field)
                        row[field] = value

            # default value for the whole resource
            for field, value in self.default_values.items():
                if value and not row.get(field, ""):
                    self.issues.log_issue(field, "default-value", value)
                    row[field] = value

            yield block
