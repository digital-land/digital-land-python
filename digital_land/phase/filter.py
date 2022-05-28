import re
from .phase import Phase


class FilterPhase(Phase):
    """
    filter rows based on field values
    """

    def __init__(self, filters={}):
        self.filters = {}
        for field, pattern in filters.items():
            self.filters[field] = re.compile(pattern)

    def process(self, stream):
        for block in stream:
            include = True
            row = block["row"]

            for field in row:
                if field in self.filters:
                    include = self.filters[field].match(row[field])
                    print(field, row[field], include)

            if include:
                yield block
