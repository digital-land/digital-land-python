import re
from .phase import Phase


class PatchPhase(Phase):
    """
    replace field values
    """

    patches = {}

    def __init__(self, issues, patches):
        self.issues = issues
        self.patches = patches

    def apply_patch(self, fieldname, value):
        patches = {**self.patches.get(fieldname, {}), **self.patches.get("", {})}
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

            for field in row:
                row[field] = self.apply_patch(field, row[field])

            yield block
