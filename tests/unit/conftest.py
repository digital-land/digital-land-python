class FakeDictReader:
    # Can be used in place of load.py::DictReaderInjectResource (a subclass of
    # csv.DictReader). Simply returns values from the passed in list of rows.
    def __init__(self, rows, resource=""):
        self.resource = resource
        self.fieldnames = rows[0].keys()
        self.rows = iter(rows)

    def __next__(self):
        return {
            "resource": self.resource,
            "row": next(self.rows),
        }

    def __iter__(self):
        return self
