import os
import glob
import re
import logging
from .register import Register, Item, hash_value


log_dir = "collection/log"
resource_dir = "collection/resource/"
endpoint_path = "collection/endpoint.csv"


class LogItem(Item):
    schema = "log"

    def migrate(self):
        # default entry-date field
        if "datetime" in self.item:
            if "entry-date" not in self.item:
                self.item["entry-date"] = self.item["datetime"]
            del self.item["datetime"]

        # default endpoint value
        if "endpoint" not in self.item:
            self.item["endpoint"] = hash_value(self.item["url"])

    def serialise(self):
        item = self.item
        del item["endpoint"]
        return item

    def check_path(self, path):
        m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
        (date, endpoint) = m.groups()

        if not self.item["entry-date"].startswith(date):
            logging.warning(
                "incorrect date in path %s for entry-date %s"
                % (path, self.item["entry-date"])
            )

        if endpoint != self.item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s"
                % (path, self.item["endpoint"])
            )


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/log/
class LogRegister(Register):
    register = "log"
    fieldnames = [
        "elapsed",
        "endpoint",
        "request-headers",
        "resource",
        "response-headers",
        "status",
        "entry-date",
        "start-date",
        "end-date",
    ]

    def load_collection(self, log_dir):
        for path in glob.glob("%s/*/*.json" % (log_dir)):
            item = LogItem()
            item.load_json(path)
            item.check_path(path)
            self.add(item)


class Collection:
    def __init__(self, log_dir=log_dir, resource_dir=resource_dir):
        self.log = LogRegister()
        self.log_dir = log_dir
        self.resource_dir = resource_dir

    def load(self):
        self.log.load_collection(self.log_dir)

    def resources(self, pipeline=None):
        resources = {}
        for entry in self.log.entries:
            if "resource" in entry.item:
                resources[entry.item["resource"]] = True
        return sorted(resources)

    def resource_path(self, resource):
        return os.path.join(self.resource_dir, resource)
