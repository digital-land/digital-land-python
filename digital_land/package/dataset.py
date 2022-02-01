import csv
import logging
from .sqlite import SqlitePackage

logger = logging.getLogger(__name__)

# TBD: move to from specification datapackage definition
tables = {
    "dataset-resource": None,
    "column-field": None,
    "issue": None,
    "entity": None,
    "old-entity": None,
    "fact": None,
    "fact-resource": None,
}

# TBD: infer from specification dataset
indexes = {
    "old-entity": ["entity", "old-entity", "status"],
    "fact": ["entity"],
    "fact-resource": ["resource", "fact"],
    "issue": ["resource", "dataset", "line-number", "entry-number", "field", "issue-type"],
}


class DatasetPackage(SqlitePackage):
    def __init__(self, dataset, **kwargs):
        self.dataset = dataset
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)

    def load_facts(self, path):
        logging.info(f"loading facts from {path}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]

        for row in csv.DictReader(open(path, newline="")):
            self.insert("fact", fact_fields, row, upsert=True)
            self.insert("fact-resource", fact_resource_fields, row, upsert=True)

        self.commit()
        self.disconnect()

    def load(self):
        pass
