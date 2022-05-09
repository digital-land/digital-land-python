class FakeDictReader:
    # Can be used in place of load.py::DictReaderInjectResource (a subclass of
    # csv.DictReader). Simply returns values from the passed in list of rows.
    def __init__(self, rows, resource=None, dataset=None):
        self.resource = resource
        self.dataset = resource
        self.fieldnames = rows[0].keys()
        self.rows = iter(rows)
        self.line_number = 0
        self.entry_number = 0

    def __next__(self):
        self.line_number += 1
        self.entry_number += 1
        return {
            "resource": self.resource,
            "dataset": self.dataset,
            "row": next(self.rows),
            "line-number": self.line_number,
            "entry-number": self.entry_number,
        }

    def __iter__(self):
        return self
