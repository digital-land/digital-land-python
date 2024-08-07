#
#  normalise CSV file formatting
#
import os
import re
import csv
from .phase import Phase

patch_dir = os.path.join(os.path.dirname(__file__), "../patch")


class NormalisePhase(Phase):
    spaces = " \n\r\t\f"
    null_patterns = []
    skip_patterns = []
    null_path = os.path.join(patch_dir, "null.csv")

    def __init__(self, skip_patterns=[], null_path=None):
        if null_path:
            self.null_path = null_path

        self.skip_patterns = []
        for pattern in skip_patterns:
            self.skip_patterns.append(re.compile(pattern))

        for row in csv.DictReader(open(self.null_path, newline="")):
            self.null_patterns.append(re.compile(row["pattern"]))

    def normalise_whitespace(self, row):
        return [
            v.strip(self.spaces).replace("\r", "").replace("\n", "\r\n") for v in row
        ]

    def strip_nulls(self, row):
        for pattern in self.null_patterns:
            row = [pattern.sub("", v) for v in row]
        return row

    def skip(self, row):
        line = ",".join(row)
        for pattern in self.skip_patterns:
            if pattern.match(line):
                return True
        return False

    def process(self, stream):
        for block in stream:
            line = block["line"]
            line = self.normalise_whitespace(line)
            line = self.strip_nulls(line)

            # skip blank lines
            if not "".join(line):
                continue

            if self.skip(line):
                continue

            block["line"] = line

            yield block
