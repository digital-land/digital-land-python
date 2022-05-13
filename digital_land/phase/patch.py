import re
from .phase import Phase


class PatchPhase(Phase):
    patch = {}

    def __init__(
        self,
        issues=None,
        patches={},
    ):
        self.issues = issues
        self.patch = patches

    def apply_patch(self, fieldname, value):
        patches = {**self.patch.get(fieldname, {}), **self.patch.get("", {})}
        for pattern, replacement in patches.items():
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if match:
                newvalue = match.expand(replacement)
                if newvalue != value:
                    self.issues.log_issue(fieldname, "patch", value)
                return newvalue

        return value

    def process(self, stream):
        for block in stream:
            row = block["row"]

            self.issues.dataset = block["dataset"]
            self.issues.resource = block["resource"]
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            for field in row:
                row[field] = self.apply_patch(field, row[field])

            yield block
