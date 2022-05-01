import re
from .phase import Phase


class FilterPhase(Phase):
    """
    remove rows which match the filter pattern
    """

    filter_patterns = {}

    def __init__(self, filter_patterns={}):
        self.filter_patterns = {}
        for field, pattern in filter_patterns.items():
            self.filter_patterns[field] = re.compile(pattern)

    def process(self, stream):
        for block in stream:
            skip = False
            row = block["row"]

            for field in row:
                if field in self.filter_patterns and self.filter_patterns[field].match(
                    row[field]
                ):
                    skip = True
                    break

            if skip:
                continue

            yield block
