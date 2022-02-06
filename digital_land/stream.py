import csv
from pathlib import Path


class Stream:
    def __init__(self, path, f=None, resource=None):
        if not f:
            f = open(path, newline="")

        if not resource:
            resource = Path(path).stem

        self.path = path
        self.f = f
        self.resource = resource
        self.line_number = 0

    def csvstream(self, f):
        for block in f:
            yield block.replace("\0", "")

    def next(self):
        for line in csv.reader(self.csvstream(self.f)):
            self.line_number = self.line_number + 1

            yield {
                "path": self.path,
                "resource": self.resource,
                "line": line,
                "line-number": self.line_number,
            }

    def __next__(self):
        return next(self.next())

    def __iter__(self):
        return self
