import csv
from pathlib import Path


class Stream:
    def __init__(self, path, f=None, resource=None, dataset=None):
        if not f:
            f = open(path, newline="")

        if not resource:
            resource = Path(path).stem

        self.path = path
        self.f = f
        self.resource = resource
        self.dataset = dataset
        self.line_number = 0

    @staticmethod
    def _reader(f):
        for line in f:
            yield line.replace("\0", "")

    def next(self):
        for line in csv.reader(self._reader(self.f)):
            self.line_number = self.line_number + 1

            yield {
                "dataset": self.dataset,
                "path": self.path,
                "resource": self.resource,
                "line": line,
                "line-number": self.line_number,
                "row": {},
            }

    def __next__(self):
        return next(self.next())

    def __iter__(self):
        return self
