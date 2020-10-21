import os
import glob
import re
import logging
from .register import Register, Item, hash_value


class LogItem(Item):
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
    key = "endpoint"
    dirname = "collection/"
    fieldnames = [
        "endpoint",
        "elapsed",
        "request-headers",
        "resource",
        "response-headers",
        "status",
        "entry-date",
        "start-date",
        "end-date",
    ]
    Item = LogItem

    def load_collection(self, log_dir="collection/log/"):
        for path in glob.glob("%s/*/*.json" % (log_dir)):
            item = self.Item()
            item.load_json(path)
            item.check_path(path)
            self.add(item)


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/source/
class EndpointRegister(Register):
    register = "endpoint"
    dirname = "collection/"
    fieldnames = [
        "endpoint",
        "endpoint-url",
        "plugin",
        "parameters",
        "entry-date",
        "start-date",
        "end-date",
    ]


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/source/
class SourceRegister(Register):
    register = "source"
    dirname = "collection/"
    key = "endpoint"
    fieldnames = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "status",
        "entry-date",
        "start-date",
        "end-date",
    ]


# this is a DataPackage ..
class Collection:
    dirname = "collection/"

    def __init__(self):
        self.source = SourceRegister()
        self.endpoint = EndpointRegister()
        self.log = LogRegister()

    def load(self):
        self.log.load_collection()
        self.endpoint.load()
        self.source.load()

    def resources(self, pipeline=None):
        resources = {}
        for entry in self.log.entries:
            if "resource" in entry.item:
                resources[entry.item["resource"]] = True
        return sorted(resources)

    def resource_organisation(self, resource):
        "return the list of organisations for which a resource was collected"
        endpoints = {}
        organisations = {}

        # entries which collected the resource
        for entry in self.log.entries:
            if "resource" in entry.item and entry.item["resource"] == resource:
                endpoints[entry.item["endpoint"]] = True

        # sources which cite the endpoint
        for endpoint in endpoints:
            for n in self.source.record[endpoint]:
                organisations[self.source.entries[n].item["organisation"]] = True
        return sorted(organisations)

    def resource_path(self, resource):
        return os.path.join(self.dirname, "resource", resource)
