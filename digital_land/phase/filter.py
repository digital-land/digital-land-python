import re
from .phase import Phase


class FilterPhase(Phase):
    """
    filter rows based on field values
    """

    def __init__(self, filter_patterns={}):
        self.filter_patterns = {}
        for field, pattern in filter_patterns.items():
            self.filter_patterns[field] = re.compile(pattern)

    def process(self, stream):
        for block in stream:
            include = True
            row = block["row"]

            for field in row:
                if field in self.filter_patterns:
                    include = self.filter_patterns[field].match(row[field])
                    print(field, row[field], include)

            if include:
                yield block
