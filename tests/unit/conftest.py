def pytest_configure(config):
    # Pre-warm the gdal version cache before any test runs. On macOS, calling
    # fork() (via subprocess) after a spatial library such as mod_spatialite has
    # been loaded causes a segmentation fault. Caching the result here ensures
    # get_gdal_version() never needs to fork after that point.
    try:
        from digital_land.utils.gdal_utils import get_gdal_version

        get_gdal_version()
    except Exception:
        pass


class FakeDictReader:
    # Can be used in place of load.py::DictReaderInjectResource (a subclass of
    # csv.DictReader). Simply returns values from the passed in list of rows.
    def __init__(self, rows, resource=None, dataset=None):
        self.resource = resource
        self.dataset = dataset
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
