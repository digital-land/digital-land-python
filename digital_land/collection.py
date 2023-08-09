import hashlib
import logging

from datetime import datetime
from pathlib import Path

from .makerules import pipeline_makerules
from .register import hash_value, Item
from .schema import Schema
from .store.csv import CSVStore
from .store.item import ItemStore

# rename and change variable
DEFAULT_COLLECTION_DIR = "./collection"


def isodate(s):
    return datetime.fromisoformat(s).strftime("%Y-%m-%d")


def resource_path(resource, directory=DEFAULT_COLLECTION_DIR):
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

    def check_item_path(self, item, in_path):
        # m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
        # (date, endpoint) = m.groups()
        path = Path(in_path)
        date = path.parts[-2]
        endpoint = path.parts[-1].split(".")[0]

        if not item.get("entry-date", "").startswith(date):
            logging.warning(
                "incorrect date in path %s for entry-date %s"
                % (path, item["entry-date"])
            )
            return False

        # print(item["url"], hash_value(item["url"]))
        if endpoint != item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
            )
            return False

        return True


# a register of resources constructed from the log register
class ResourceLogStore(CSVStore):
    def load(
        self, log: LogStore, source: CSVStore, directory: str = DEFAULT_COLLECTION_DIR
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


class SourceStore(CSVStore):
    def __init__(self, schema=Schema("source")):
        super().__init__(schema=schema)

    def add_entry(self, entry):
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

        if self.validate_entry(item):
            super().add_entry(item)
            # the below is a good idea but I'm not too sure on how best to implement
            # self.new_sources.append(item)

    @staticmethod
    def validate_entry(source_item: Item = None) -> bool:
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


class EndpointStore(CSVStore):
    def __init__(self, schema=Schema("endpoint")):
        super().__init__(schema=schema)

    def add_entry(self, entry):
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

    @staticmethod
    def validate_entry(endpoint_item: Item) -> bool:
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


# expected this will be based on a Datapackage class
class Collection:
    def __init__(self, name=None, directory=DEFAULT_COLLECTION_DIR):
        self.name = name

        # let's move all directory variable set up to the top of the class
        # setting default directories here in one place makes the code easier to read
        # shortened directory to dir less typing
        # setting log_dir here means same log_dir default is used across class rather than setting it where neccessary
        self.dir = directory
        self.log_dir = Path(directory) / "log/*/"

        # define the set of classes up front again easier to read
        self.log = LogStore(Schema("log"))
        self.resource = ResourceLogStore(Schema("resource"))
        self.old_resource = CSVStore(Schema("old-resource"))

        # following on from the comments below I have added some extra intermediate classes
        # for source and endpoint, bfore we merge we can investigate possible shifting/generalising their functionality to the CSVStore class
        # but given that this may be difficult for now I haven't
        self.source = CSVStore(Schema("source"))
        self.endpoint = CSVStore(Schema("endpoint"))

        #  michael's new variables
        # self.validate = self.Validate()
        # self.new_sources = []
        # self.new_endpoints = []
        # self.new_lookups = []

    def load_log_items(self, directory=None, log_directory=None):
        directory = directory or self.dir
        log_directory = log_directory = self.log_dir

        logging.info("loading log files")
        #  moved to top of class
        # self.log = LogStore(Schema("log"))
        self.log.load(directory=log_directory)

        logging.info("indexing resources")
        # moved to top of class
        # self.resource = ResourceLogStore(Schema("resource"))
        self.resource.load(log=self.log, source=self.source, directory=directory)

    def save_csv(self, directory=None):
        logging.info("saving csv")
        # use consistent method for setting optional variables in class
        directory = directory or self.dir
        # if not directory:
        #     directory = self.directory

        self.endpoint.save_csv(directory=directory)
        self.source.save_csv(directory=directory)
        self.log.save_csv(directory=directory)
        self.resource.save_csv(directory=directory)

    def load(self, directory=None):
        directory = directory or self.dir

        # moved to top
        # self.source = CSVStore(Schema("source"))
        self.source.load(directory=directory)

        # moved to top
        # self.endpoint = CSVStore(Schema("endpoint"))
        self.endpoint.load(directory=directory)

        try:
            # moved to top (also worth noting that log was being used to store two separate things a CSVSotre and a LogSotre
            # this feels like an inconsistent method to me and is very unreadable as depending on the order of functions
            # triggered self.log represents different things it's worth noting that Logstore can use many of the methods of csv store
            # self.log = CSVStore(Schema("log"))
            # now we are always defining self.log as a log store this changes the behaviour of the load method below
            # so we replace this with the relevant function from the logstore class
            # self.log.load(directory=directory)
            self.log.load_csv(directory=directory)

            # same as above where the type of class has changed let's address this and again use consistent classes
            # self.resource = CSVStore(Schema("resource"))
            # self.resource.load(directory=directory)
            self.resource.load_csv(directory=directory)
        except FileNotFoundError:
            self.load_log_items()

        try:
            # move to top
            # self.old_resource = CSVStore(Schema("old-resource"))
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

    # endpoint-url should be included in the entry not to sure why it would be seperate?
    def add_source_endpoint(self, entry: dict):
        # you can just make the arguement not optional (an arg not a kwarg) and it will throw an error
        # if not entry:
        #     return
        # if not endpoint_url:
        #     return
        if not entry.get("collection"):
            entry["collection"] = self.name
        # again endpoint-url can come from the entry
        # entry["endpoint-url"] = endpoint_url

        # check that entry contains allowed columns names
        allowed_names = set(
            # list(Schema("endpoint").fieldnames) + list(Schema("source").fieldnames)
            list(self.endpoint.schema.fieldnames)
            + list(self.source.schema.fieldnames)
        )

        # do we care if there are extra columns? we only really care if they're missing the ones we need I suppose it can't hurt
        for entry_key in entry.keys():
            if entry_key not in allowed_names:
                logging.error(f"unrecognised argument '{entry_key}'")
                continue

        # add entries to source and endpoint csvs changed this just to add to the stores but not to save to csv
        # hash_value should be added in the functions beow
        entry["endpoint"] = hash_value(entry["endpoint-url"])

        if not entry.get("end-date"):
            entry["end-date"] = ""

        # entry["end-date"] = entry.get("end-date", "")

        if not entry.get("pipelines"):
            entry["pipelines"] = entry["collection"]

        # entry["pipelines"] = entry.get("pipelines", entry["collection"])

        self.add_endpoint(entry)
        self.add_source(entry)

        self.recalculate_source_hashes()
        #  I argue that we shouldn't save it to the csv here. otherwise it makes it hard
        # self.save_csv()

    # while having a method to add a source to the collection isn't the worst idea I would suggest
    # expanding the functionality of the CSVSore to handle the bulk of it. This means that the class representing endpoints
    # or sources could be used outside of the collection class to achieve the same thing
    # the collection class is am wrapper for the set of underlying classes
    # modularising this wrap means that it may be easier to unit test
    # changed definition so argument isn't optional
    # def add_source(self, entry: dict = None):
    def add_source(self, entry: dict):
        # item = Item(
        #     {
        #         "source": entry.get("source", ""),
        #         "collection": entry["collection"],
        #         "pipelines": entry.get("pipelines", entry["collection"]),
        #         "organisation": entry.get("organisation", ""),
        #         "endpoint": entry["endpoint"],
        #         "documentation-url": entry.get("documentation-url", ""),
        #         "licence": entry.get("licence", ""),
        #         "attribution": entry.get("attribution", ""),
        #         "entry-date": self.entry_date(entry),
        #         "start-date": self.start_date(entry),
        #         "end-date": self.end_date(entry),
        #     }
        # )

        # if self.validate.source_entry(item):
        #     self.source.add_entry(item)
        #     self.new_sources.append(item)
        self.source.add_entry(entry)

    #
    def add_endpoint(self, entry: dict):
        # item = Item(
        #     {
        #         "endpoint": entry["endpoint"],
        #         "endpoint-url": entry["endpoint-url"],
        #         "plugin": entry.get("plugin", ""),
        #         "parameters": entry.get("parameters", ""),
        #         "entry-date": self.entry_date(entry),
        #         "start-date": self.start_date(entry),
        #         "end-date": self.end_date(entry),
        #     }
        # )

        # if self.validate.endpoint_entry(item):
        #     self.endpoint.add_entry(item)
        #     self.new_endpoints.append(item)
        self.endpoint.add_entry(entry)

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

        # while having a class structure for validattions could be a good idea we already have something akin to this in the datatype
        # module before creating something new we should look at using that where possible. For now I have added these into the new proposed
        # structure above we may be able to generalise and pull in code from datatypes later
        # class Validate:
        """
        This validator class provides a centralised place to perform
        checks against fields used during the collection process
        """
        # do you need an init function if it does nothing?
        # def __init__(self):
        #     pass

        # moved to csv store so any class can use it to validate a url
        # @staticmethod
        # def endpoint_url(endpoint_val: str = None) -> bool:
        #     """
        #     Checks whether the supplied parameter is valid according to
        #     business rules
        #     :param endpoint_val:
        #     :return: Boolean
        #     """
        #     if not endpoint_val:
        #         return False
        #     if type(endpoint_val) is float:
        #         return False

        #     return True

        # moved to csv store so any class can use it to validate a url
        # @staticmethod
        # def organisation_name(org_name_val: str = None) -> bool:
        #     """
        #     Checks whether the supplied parameter is valid according to
        #     business rules
        #     :param org_name_val:
        #     :return: Boolean
        #     """
        #     if not org_name_val:
        #         return False
        #     if type(org_name_val) is float:
        #         return False

        #     return True

        # not convinced the naming is appropriate but I've moved it to the CSV store class for now
        # @staticmethod
        # def reference(ref_val: str = None) -> bool:
        #     """
        #     Checks whether the supplied parameter is valid according to
        #     business rules
        #     :return: Boolean
        #     """
        #     if not ref_val:
        #         return False
        #     if type(ref_val) is float:
        #         return False

        #     return True

        # moved into endpoint store class
        # @staticmethod
        # def endpoint_entry(endpoint_item: Item = None) -> bool:
        #     """
        #     Checks whether the supplied parameter is valid according to
        #     business rules
        #     :return: Boolean
        #     """
        #     if not endpoint_item:
        #         return False
        #     if not endpoint_item["endpoint"]:
        #         return False
        #     if not endpoint_item["endpoint-url"]:
        #         return False

        #     return True
