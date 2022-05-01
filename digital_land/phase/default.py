from .phase import Phase


class DefaultFieldPhase(Phase):
    """
    default missing fields from another field
    """

    def __init__(self, issues, fieldnames={}):
        self.issues = issues
        self.fieldnames = fieldnames

    def process(self, stream):
        for block in stream:
            row = block["row"]

            for field, default_fieldnames in self.fieldnames.items():
                for default_field in default_fieldnames:
                    if not row.get(field, ""):
                        value = row.get(default_field, "")
                        if value:
                            self.issues.log_issue(field, "default field", default_field)
                            row[field] = value

            yield block
