import hashlib
import logging
import re

from datetime import datetime
from pathlib import Path

from .makerules import pipeline_makerules
from .register import hash_value, Item
from .schema import Schema
from .store.csv import CSVStore
from .store.item import ItemStore

collection_directory = "./collection"


def isodate(s):
    return datetime.fromisoformat(s).strftime("%Y-%m-%d")


def resource_path(resource, directory=collection_directory):
    return Path(directory) / "resource" / resource


def resource_url(collection, resource):
    return (
        "https://raw.githubusercontent.com/digital-land/"
        + "%s-collection/master/collection/resource/%s" % (collection, resource)
    )


# a store of log files created by the collector
# collecton/log/YYYY-MM-DD/{endpoint#}.json
class LogStore(ItemStore):
    def load_item(self, path):
        item = super().load_item(path)
        item = item.copy()

        # migrate content-type and bytes fields
        h = item.get("response-headers", {})
        if "content-type" not in item:
            item["content-type"] = h.get("Content-Type", "")
        if "bytes" not in item:
            item["bytes"] = h.get("Content-Length", "")

        # migrate datetime to entry-date field
        if "datetime" in item:
            if "entry-date" not in item:
                item["entry-date"] = item["datetime"]
            del item["datetime"]

        # migrate url to endpoint-url field
        if "url" in item:
            if "endpoint-url" not in item:
                item["endpoint-url"] = item["url"]
            del item["url"]

        # default the endpoint value
        if "endpoint" not in item:
            item["endpoint"] = hash_value(item["endpoint-url"])
        self.check_item_path(item, path)

        return item

    def save_item(self, item, path):
        del item["endpoint"]
        return super().save_item(item)

    def check_item_path(self, item, path):
        m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
        (date, endpoint) = m.groups()

        if not item.get("entry-date", "").startswith(date):
            logging.warning(
                "incorrect date in path %s for entry-date %s"
                % (path, item["entry-date"])
            )

        # print(item["url"], hash_value(item["url"]))
        if endpoint != item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
            )


# a register of resources constructed from the log register
class ResourceLogStore(CSVStore):
    def load(
        self, log: LogStore, source: CSVStore, directory: str = collection_directory
    ):
        """
        Rebuild resource.csv file from the log store

        This does not depend in any way on the current state of resource.csv on the file system

        We cannot assume that all resources are present on the local file system as we also want to keep records
        of resources we have not collected within the current collector execution due to their end_date elapsing

        :param log:
        :type log: LogStore
        :param source:
        :type source: CSVStore
        :param directory:
        :type directory: str
        """
        resources = {}
        today = datetime.utcnow().isoformat()[:10]

        for entry in log.entries:
            if "resource" in entry:
                resource = entry["resource"]
                if resource not in resources:
                    resources[resource] = {
                        "bytes": entry["bytes"],
                        "endpoints": {},
                        "start-date": entry["entry-date"],
                        "end-date": entry["entry-date"],
                    }
                else:
                    resources[resource]["start-date"] = min(
                        resources[resource]["start-date"], entry["entry-date"]
                    )
                    resources[resource]["end-date"] = max(
                        resources[resource]["end-date"], entry["entry-date"]
                    )
                resources[resource]["endpoints"][entry["endpoint"]] = True

        for key, resource in sorted(resources.items()):
            organisations = set()
            datasets = set()
            for endpoint in resource["endpoints"]:
                for entry in source.records[endpoint]:
                    organisations.add(entry["organisation"])
                    datasets = set(
                        entry.get("datasets", entry.get("pipelines", "")).split(";")
                    )

            end_date = isodate(resource["end-date"])
            if end_date >= today:
                end_date = ""

            self.add_entry(
                {
                    "resource": key,
                    "bytes": resource["bytes"],
                    "endpoints": ";".join(sorted(resource["endpoints"])),
                    "organisations": ";".join(sorted(organisations)),
                    "datasets": ";".join(sorted(datasets)),
                    "start-date": isodate(resource["start-date"]),
                    "end-date": end_date,
                }
            )


# expected this will be based on a Datapackage class
class Collection:
    def __init__(self, name=None, directory=collection_directory):
        self.name = name
        self.directory = directory
        self.validate = self.Validate()
        self.new_sources = []
        self.new_endpoints = []
        self.new_lookups = []

    def load_log_items(self, directory=None, log_directory=None):
        if not log_directory:
            directory = directory or self.directory
            log_directory = Path(directory) / "log/*/"

        logging.info("loading log files")
        self.log = LogStore(Schema("log"))
        self.log.load(directory=log_directory)

        logging.info("indexing resources")
        self.resource = ResourceLogStore(Schema("resource"))
        self.resource.load(log=self.log, source=self.source, directory=directory)

    def save_csv(self, directory=None):
        logging.info("saving csv")
        if not directory:
            directory = self.directory

        self.endpoint.save_csv(directory=directory)
        self.source.save_csv(directory=directory)
        self.log.save_csv(directory=directory)
        self.resource.save_csv(directory=directory)

    def load(self, directory=None):
        directory = directory or self.directory

        self.source = CSVStore(Schema("source"))
        self.source.load(directory=directory)

        self.endpoint = CSVStore(Schema("endpoint"))
        self.endpoint.load(directory=directory)

        try:
            self.log = CSVStore(Schema("log"))
            self.log.load(directory=directory)

            self.resource = CSVStore(Schema("resource"))
            self.resource.load(directory=directory)
        except FileNotFoundError:
            self.load_log_items()

        try:
            self.old_resource = CSVStore(Schema("old-resource"))
            self.old_resource.load(directory=directory)
        except FileNotFoundError:
            pass

    def resource_endpoints(self, resource):
        "the list of endpoints a resource was collected from"
        return self.resource.records[resource][-1]["endpoints"].split(";")

    def resource_start_date(self, resource):
        "the first date a resource was collected"
        return self.resource.records[resource][-1]["start-date"]

    def resource_organisations(self, resource):
        "the list of organisations for which a resource was collected"
        return self.resource.records[resource][-1]["organisations"].split(";")

    def resource_path(self, resource):
        return resource_path(resource, self.directory)

    def pipeline_makerules(self):
        pipeline_makerules(self)

    def dataset_resource_map(self):
        "a map of resources needed by each dataset in a collection"
        today = datetime.utcnow().isoformat()
        endpoint_dataset = {}
        dataset_resource = {}
        redirect = {}

        for entry in self.old_resource.entries:
            redirect[entry["old-resource"]] = entry["resource"]

        for entry in self.source.entries:
            if entry["end-date"] and entry["end-date"] > today:
                continue

            endpoint_dataset.setdefault(entry["endpoint"], set())
            datasets = entry.get("datasets", "") or entry.get("pipelines", "")
            for dataset in datasets.split(";"):
                if dataset:
                    endpoint_dataset[entry["endpoint"]].add(dataset)

        for entry in self.resource.entries:
            if entry["end-date"] and entry["end-date"] > today:
                continue

            for endpoint in entry["endpoints"].split(";"):
                for dataset in endpoint_dataset[endpoint]:
                    # ignore or redirect a resource in the old-resource table
                    resource = entry["resource"]
                    resource = redirect.get(resource, resource)
                    if resource:
                        dataset_resource.setdefault(dataset, set())
                        dataset_resource[dataset].add(resource)
        return dataset_resource

    @staticmethod
    def format_date(date_val) -> str:
        if type(date_val) is datetime:
            str_date_fmt = date_val.strftime("%Y-%m-%d")
        elif type(date_val) is int:
            param_start_date_str = str(date_val)
            param_start_date_dt = datetime.strptime(param_start_date_str, "%Y%m%d")
            if type(param_start_date_dt) is datetime:
                str_date_fmt = param_start_date_dt.strftime("%Y-%m-%d")
        else:
            str_date_fmt = datetime.now().strftime("%Y-%m-%d")

        return str_date_fmt

    def add_source_endpoint(self, entry: dict = None, endpoint_url: str = None):
        if not entry:
            return
        if not endpoint_url:
            return

        entry["collection"] = self.name
        entry["endpoint-url"] = endpoint_url

        # check that entry contains allowed columns names
        allowed_names = set(
            list(Schema("endpoint").fieldnames) + list(Schema("source").fieldnames)
        )

        for entry_key in entry.keys():
            if entry_key not in allowed_names:
                logging.error(f"unrecognised argument '{entry_key}'")
                continue

        # add entries to source and endpoint csvs
        entry["endpoint"] = hash_value(entry["endpoint-url"])

        self.add_endpoint(entry)
        self.add_source(entry)

        self.recalculate_source_hashes()
        self.save_csv()

    def add_source(self, entry: dict = None):
        item = Item(
            {
                "source": entry.get("source", ""),
                "collection": entry["collection"],
                "pipelines": entry.get("pipelines", entry["collection"]),
                "organisation": entry.get("organisation", ""),
                "endpoint": entry["endpoint"],
                "documentation-url": entry.get("documentation-url", ""),
                "licence": entry.get("licence", ""),
                "attribution": entry.get("attribution", ""),
                "entry-date": self.entry_date(entry),
                "start-date": self.start_date(entry),
                "end-date": self.end_date(entry),
            }
        )

        if self.validate.source_entry(item):
            self.source.add_entry(item)
            self.new_sources.append(item)

    def add_endpoint(self, entry: dict = None):
        item = Item(
            {
                "endpoint": entry["endpoint"],
                "endpoint-url": entry["endpoint-url"],
                "plugin": entry.get("plugin", ""),
                "parameters": entry.get("parameters", ""),
                "entry-date": self.entry_date(entry),
                "start-date": self.start_date(entry),
                "end-date": self.end_date(entry),
            }
        )

        if self.validate.endpoint_entry(item):
            self.endpoint.add_entry(item)
            self.new_endpoints.append(item)

    def recalculate_source_hashes(self):
        for entry in self.source.entries:
            key = "%s|%s|%s" % (
                entry["collection"],
                entry["organisation"],
                entry["endpoint"],
            )
            entry["source"] = hashlib.md5(key.encode()).hexdigest()

    def start_date(self, entry):
        if entry.get("start-date", ""):
            return datetime.strptime(entry["start-date"], "%Y-%m-%d").date()
        return ""

    def end_date(self, entry):
        if entry.get("end-date", ""):
            return datetime.strptime(entry["end-date"], "%Y-%m-%d").date()
        return ""

    def entry_date(self, entry):
        return entry.get(
            "entry-date", datetime.utcnow().strftime("%Y-%m-%dT%H:%H:%M:%SZ")
        )

    class Validate:
        """
        This validator class provides a centralised place to perform
        checks against fields used during the collection process
        """

        def __init__(self):
            pass

        @staticmethod
        def endpoint_url(endpoint_val: str = None) -> bool:
            """
            Checks whether the supplied parameter is valid according to
            business rules
            :param endpoint_val:
            :return: Boolean
            """
            if not endpoint_val:
                return False
            if type(endpoint_val) is float:
                return False

            return True

        @staticmethod
        def organisation_name(org_name_val: str = None) -> bool:
            """
            Checks whether the supplied parameter is valid according to
            business rules
            :param org_name_val:
            :return: Boolean
            """
            if not org_name_val:
                return False
            if type(org_name_val) is float:
                return False

            return True

        @staticmethod
        def reference(ref_val: str = None) -> bool:
            """
            Checks whether the supplied parameter is valid according to
            business rules
            :return: Boolean
            """
            if not ref_val:
                return False
            if type(ref_val) is float:
                return False

            return True

        @staticmethod
        def source_entry(source_item: Item = None) -> bool:
            """
            Checks whether the supplied parameter is valid according to
            business rules
            :return: Boolean
            """
            if not source_item:
                return False
            if not source_item["collection"]:
                return False
            if not source_item["organisation"]:
                return False
            if not source_item["endpoint"]:
                return False

            return True

        @staticmethod
        def endpoint_entry(endpoint_item: Item = None) -> bool:
            """
            Checks whether the supplied parameter is valid according to
            business rules
            :return: Boolean
            """
            if not endpoint_item:
                return False
            if not endpoint_item["endpoint"]:
                return False
            if not endpoint_item["endpoint-url"]:
                return False

            return True
