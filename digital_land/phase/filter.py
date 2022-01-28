import re
from .phase import Phase


class FilterPhase(Phase):
    """
    filter out rows based on field values
    """

    filter_patterns = {}

    def __init__(self, filter_patterns={}):
        self.filter_patterns = {}
        for field, pattern in filter_patterns.items():
            self.filter_patterns[field] = re.compile(pattern)

    def process(self, reader):
        for stream_data in reader:
            skip = False
            row = stream_data["row"]

            for field in row:
                if field in self.filter_patterns and self.filter_patterns[field].match(
                    row[field]
                ):
                    skip = True
                    break

            if skip:
                continue

            yield stream_data
