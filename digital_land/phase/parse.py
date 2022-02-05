from .phase import Phase


class ParsePhase(Phase):
    """
    convert lines of text into rows of fields
    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.fieldnames = None
        self.line_stream = None
        self.line_number = 0

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.line_stream)
        self.line_number += 1
        data = {
            "resource": line["resource"],
            "dataset": self.dataset,
            "row": dict(zip(self.fieldnames, line["line"])),
            "line-number": self.line_number,
        }
        return data

    def process(self, line_stream):
        try:
            stream_data = next(line_stream)
        except StopIteration:
            self.fieldnames = []
            self.line_stream = iter(())
            return self

        self.fieldnames = stream_data["line"]
        self.line_stream = line_stream
        return self
