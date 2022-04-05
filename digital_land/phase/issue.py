from .phase import Phase


class IssuePhase(Phase):
    """
    add the stream context to the issue_log
    """

    def __init__(self, dataset, issues):
        self.dataset = dataset
        self.issues = issues

    def process(self, stream):
        for block in stream:
            resource = block["resource"]

            self.issues.dataset = self.dataset
            self.issues.resource = resource
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            yield block
