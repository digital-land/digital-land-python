import re
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

    def load_issues(self, path, resource):
        fields = self.specification.schema["issue"]["fields"]

        logging.info(f"loading issues from {path}")

        for row in csv.DictReader(open(path, newline="")):
            row["resource"] = resource
            row["pipeline"] = self.dataset
            self.insert("issue", fields, row)

    def load_transformed(self, path):
        m = re.search(r"/([a-f0-9]+).csv$", path)
        resource = m.group(1)

        self.connect()

        self.create_cursor()
        self.load_facts(path)
        self.load_issues(path.replace("transformed/", "issue/"), resource)
        self.commit()

        self.disconnect()

    def load(self):
        pass
