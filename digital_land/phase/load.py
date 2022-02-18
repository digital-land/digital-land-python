import csv
from pathlib import Path
from .phase import Phase


class Stream:
    def __init__(self, path=None, f=None, resource=None, dataset=None, log=None):
        if not f:
            f = open(path, newline="")

        if not resource:
            if path:
                resource = Path(path).stem

        self.path = path
        self.f = f
        self.resource = resource
        self.dataset = dataset
        self.line_number = 0
        self.fieldnames = None
        self.log = log

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

        if self.log:
            if not self.log.dataset:
                self.log.dataset = self.dataset
            if not self.log.resource:
                self.log.resource = self.resource
            self.log.line_count = self.line_number

    def __next__(self):
        return next(self.next())

    def __iter__(self):
        return self


class LoadPhase(Phase):
    def __init__(self, *args, **kwargs):
        self.stream = Stream(*args, **kwargs)

    def process(self, stream=None):
        return self.stream
