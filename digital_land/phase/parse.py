from .phase import Phase


class ParsePhase(Phase):
    """
    convert stream lines of text into rows of fields
    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.fieldnames = None

    def process(self, stream):
        self.fieldnames = next(stream)
        for block in stream:
            block["dataset"] = self.dataset
            block["fieldnames"]: self.fieldnames
            block["row"] = dict(zip(self.fieldnames, block["line"]))
            yield block
