import hashlib
import logging
import os
import pandas as pd

from datetime import datetime
from pathlib import Path
import json

from .makerules import pipeline_makerules
from .register import hash_value, Item
from .schema import Schema
from .store.csv import CSVStore
from .store.item import ItemStore


logger = logging.getLogger(__name__)
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
    def __init__(self, *args, **kwargs):
        self._latest_entry_date = None
        super().__init__(*args, **kwargs)

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
        path = Path(in_path)
        date = path.parts[-2]
        endpoint = path.parts[-1].split(".")[0]

        if not item.get("entry-date", "").startswith(date):
            logging.warning(
                "incorrect date in path %s for entry-date %s"
                % (path, item["entry-date"])
            )
            return False

        if endpoint != item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
            )
            return False

        return True

    def add_entry(self, item):
        super().add_entry(item)
        # Update the _latest_entry_date
        if "entry-date" in item:
            if (
                not self._latest_entry_date
                or item["entry-date"] > self._latest_entry_date
            ):
                self._latest_entry_date = item["entry-date"]

    def latest_entry_date(self):
        """Gets the latest entry date from the log"""
        return self._latest_entry_date


# a register of resources constructed from the log register
class ResourceLogStore(CSVStore):
    def load(
        self,
        log: LogStore,
        source: CSVStore,
        directory: str = DEFAULT_COLLECTION_DIR,
        after: datetime = None,
    ):
        """
        Rebuild or update resource.csv file from the log store.

        We cannot assume that all resources are present on the local file system as we also want to keep records
        of resources we have not collected within the current collector execution due to their end_date elapsing.

        If 'after' is not None, only log entires after the given datetime will be loaded (used when updating).

        :param log:
        :type log: LogStore
        :param source:
        :type source: CSVStore
        :param directory:
        :type directory: str
        :param after:
        :type directory: datetime
        """
        resources = {}
        today = datetime.utcnow().isoformat()[:10]
        logger.setLevel(logging.INFO)

        # Process the log entries
        for entry in log.entries:
            if "resource" in entry and len(entry["resource"]):
                if after:
                    if entry["entry-date"] < after:
                        continue

                resource = entry["resource"]
                if resource not in resources:
                    resources[resource] = {
                        "bytes": entry["bytes"],
                        "endpoints": set(),
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
                resources[resource]["endpoints"].add(entry["endpoint"])

        # Convert these into resource entries to be added
        new_entries = {}

        # What is the current hash of the source.records
        try:
            record_string = json.dumps(source.records)
            record_hash = hashlib.sha256(record_string.encode("utf-8")).hexdigest()
            logger.info(f"Hash of source.records on {today} is: {record_hash}")
        except TypeError:
            logger.info("Cannot hash source.records")

        for key, resource in sorted(resources.items()):
            organisations = set()
            datasets = set()
            for endpoint in resource["endpoints"]:
                if endpoint not in source.records:
                    raise RuntimeError(
                        f"Endpoint '{endpoint}' not found in source. Check the endpoint.csv and source.csv files."
                    )

                for entry in source.records[endpoint]:
                    organisations.add(entry["organisation"])
                    datasets = set(
                        entry.get("datasets", entry.get("pipelines", "")).split(";")
                    )

            new_entries[key] = {
                "bytes": resource["bytes"],
                "endpoints": resource["endpoints"],
                "organisations": organisations,
                "datasets": datasets,
                "start-date": isodate(resource["start-date"]),
                "end-date": isodate(resource["end-date"]),
            }

        # Update existing entries
        for entry in self.entries:
            resource = entry["resource"]
            if resource in new_entries:
                new_entry = new_entries[resource]
                endpoints = set(entry["endpoints"].split(";")).union(
                    new_entry["endpoints"]
                )
                organisations = set(new_entry["organisations"])
                datasets = set(new_entry["datasets"])
                start_date = min(entry["start-date"], new_entry["start-date"])
                end_date = max(entry["end-date"], new_entry["end-date"])

                entry["endpoints"] = ";".join(sorted(endpoints))
                entry["organisations"] = ";".join(sorted(organisations))
                entry["datasets"] = ";".join(sorted(datasets))
                entry["start-date"] = start_date
                entry["end-date"] = "" if end_date >= today else end_date

                del new_entries[resource]  # Remove it from the list so we don't add it
            else:
                # tackle changes that are made in config for organisation and dataset
                organisations = set()
                datasets = set()
                for endpoint in entry["endpoints"].split(";"):
                    for r_source in source.records[endpoint]:
                        organisations.add(r_source["organisation"])
                        datasets = set(
                            r_source.get(
                                "datasets", r_source.get("pipelines", "")
                            ).split(";")
                        )
                entry["organisations"] = ";".join(sorted(organisations))
                entry["datasets"] = ";".join(sorted(datasets))

        # Add any new entries
        for resource, new_entry in new_entries.items():
            self.add_entry(
                {
                    "resource": resource,
                    "bytes": new_entry["bytes"],
                    "endpoints": ";".join(sorted(new_entry["endpoints"])),
                    "organisations": ";".join(sorted(new_entry["organisations"])),
                    "datasets": ";".join(sorted(new_entry["datasets"])),
                    "start-date": new_entry["start-date"],
                    "end-date": (
                        "" if new_entry["end-date"] >= today else new_entry["end-date"]
                    ),
                }
            )


class SourceStore(CSVStore):
    def __init__(self, schema=Schema("source")):
        super().__init__(schema=schema)

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
        if (
            not source_item["organisation"]
            and type(source_item["organisation"]) is not str
        ):
            return False
        if not source_item["endpoint"]:
            return False

        return True


class EndpointStore(CSVStore):
    def __init__(self, schema=Schema("endpoint")):
        super().__init__(schema=schema)

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
        if (
            not endpoint_item["endpoint-url"]
            and type(endpoint_item["endpoint-url"]) is not str
        ):
            return False

        return True

    def is_not_duplicate(self, endpoint_item: Item) -> bool:
        """
        check if given endpoint already exists
        :param endpoint_item:
        :return:
        """
        existing_entries = len(
            [
                1
                for item in self.entries
                if item["endpoint"] == endpoint_item["endpoint"]
                and item["endpoint-url"] == endpoint_item["endpoint-url"]
            ]
        )

        if existing_entries > 0:
            print(">>> INFO: endpoint already exists")
            print(f">>> Endpoint hash {endpoint_item['endpoint']}")
            print(f">>> Endpoint URL {endpoint_item['endpoint-url']}")
            return False

        return True


# expected this will be based on a Datapackage class
class Collection:
    def __init__(self, name=None, directory=DEFAULT_COLLECTION_DIR):
        self.name = name

        # setting relevant directory variables
        self.dir = Path(directory)

        # define the set of classes up front again easier to read
        self.log = LogStore(Schema("log"))
        self.resource = ResourceLogStore(Schema("resource"))
        self.old_resource = CSVStore(Schema("old-resource"))
        self.source = SourceStore()
        self.endpoint = EndpointStore()

    def load_log_items(self, directory=None, log_directory=None, after=None):
        """
        Method to load the log store and resource store from log items instead of csvs. used when csvs don't exist
        or new log items have been created by running a collector. If 'after' is not None, only log items after the
        specified date / time will be loaded.
        """
        directory = directory or self.dir
        log_directory = log_directory or Path(directory) / "log/*/"

        logging.info("loading log files")
        self.log.load(directory=log_directory, after=after)

        logging.info("indexing resources")
        self.resource.load(
            log=self.log, source=self.source, directory=directory, after=after
        )

    def save_csv(self, directory=None):
        directory = directory or self.dir

        logging.info("saving csv")
        self.endpoint.save_csv(directory=directory)
        self.source.save_csv(directory=directory)
        self.log.save_csv(directory=directory)
        self.resource.save_csv(directory=directory)

    def load(self, directory=None, refill_todays_logs=False):
        directory = directory or self.dir
        self.source.load(directory=directory)
        self.endpoint.load(directory=directory)

        regenerate_resources = False

        # Try to load log store from csv first
        try:
            self.log.load_csv(
                directory=directory, refill_todays_logs=refill_todays_logs
            )
            logging.info(f"Log loaded from CSV - {len(self.log.entries)} entries")
        except FileNotFoundError:
            logging.info("No log.csv - building from log items")
            self.load_log_items(directory=directory)
            regenerate_resources = True

        # Now try to load resources, unless we need to rebuild them anyway
        if not regenerate_resources:
            try:
                self.resource.load_csv(directory=directory)
                logging.info(
                    f"Resource loaded from CSV - {len(self.resource.entries)} entries"
                )
            except FileNotFoundError:
                logging.info("No resources.csv - generating from log.csv")
                regenerate_resources = True

        # Do we need to regenerate resources?
        if regenerate_resources:
            logging.info("Generating resources from log.csv")
            self.resource.load(log=self.log, source=self.source, directory=directory)

        # attempts to load in old-resources if the file exists, many use cases won't have any
        try:
            self.old_resource.load(directory=directory)
        except FileNotFoundError:
            pass

    def update(self):
        self.load_log_items(after=self.log.latest_entry_date())

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
        return resource_path(resource, self.dir)

    def filtered_sources(self, filter: dict):
        return [
            entry
            for entry in self.source.entries
            if all(
                (
                    v in entry.get(k, "").split(";")
                    if k == "pipelines" and ";" in str(entry.get(k, ""))
                    else entry.get(k) == v
                )
                for k, v in filter.items()
            )
        ]

    def pipeline_makerules(
        self,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path=None,
    ):
        pipeline_makerules(
            self,
            specification_dir,
            pipeline_dir,
            resource_dir,
            incremental_loading_override,
            state_path=state_path,
        )

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
                    # resource = redirect.get(resource, resource)
                    if resource:
                        dataset_resource.setdefault(dataset, set())
                        dataset_resource[dataset].add(resource)
        return dataset_resource

    @staticmethod
    def format_date(date_val) -> str:
        if type(date_val) is str:
            param_start_date_dt = None
            for fmt in ["%Y-%m-%d", "%d/%m/%Y"]:
                try:
                    param_start_date_dt = datetime.strptime(date_val, fmt)
                    break
                except ValueError:
                    pass
            str_date_fmt = (param_start_date_dt or datetime.now()).strftime("%Y-%m-%d")
        elif type(date_val) is datetime:
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
    def add_source_endpoint(self, entry: dict) -> bool:
        """
        adds entries to teh endpoint and source csvs, if validation
        checks pass.
        :param entry:
        :return: Boolean value indicating if entries were added successfully
        """
        if not entry.get("collection"):
            entry["collection"] = self.name

        # check that entry contains allowed columns names
        allowed_names = set(
            list(self.endpoint.schema.fieldnames) + list(self.source.schema.fieldnames)
        )

        # do we care if there are extra columns? we only really care if they're missing the ones we need I suppose it can't hurt
        for entry_key in entry.keys():
            if entry_key not in allowed_names:
                logging.error(f"unrecognised argument '{entry_key}'")
                continue

        # add entries to source and endpoint csvs changed this just to add to the stores but not to save to csv
        # hash_value should be added in the functions below
        entry["endpoint"] = hash_value(entry["endpoint-url"])

        if not entry.get("end-date"):
            entry["end-date"] = ""

        if not entry.get("entry-date"):
            entry["entry-date"] = datetime.now().strftime("%Y-%m-%d")

        if not self.add_endpoint(entry):
            return False

        self.add_source(entry)
        self.recalculate_source_hashes()

        return True

    def add_source(self, entry: dict):
        source_entry = Item(
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
        if self.source.validate_entry(source_entry):
            self.source.add_entry(source_entry)

    def add_endpoint(self, entry: dict) -> bool:
        endpoint_entry = Item(
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

        if self.endpoint.validate_entry(endpoint_entry):
            if self.endpoint.is_not_duplicate(endpoint_entry):
                self.endpoint.add_entry(endpoint_entry)
                return True

        return False

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
            return self.format_date(entry["start-date"])
        return ""

    def end_date(self, entry):
        if entry.get("end-date", ""):
            return self.format_date(entry["end-date"])
        return ""

    def entry_date(self, entry):
        return entry.get(
            "entry-date", datetime.utcnow().strftime("%Y-%m-%dT%H:%H:%M:%SZ")
        )

    def retire_endpoints_and_sources(collection, collection_df_to_retire) -> None:
        """
        This method should be called from commands.py with the correct input.csv format per unique collection.

        Args:
            collection (Collection): The Collection object which gets instantiated by a corresponding commands.py function which calls this method.
            collection_df_to_retire (str): A small dataframe of sources and endpoints to be retired within a colleciton, this should be
                generated by a function call within commands.py.
        """

        try:
            endpoint_csv_path = os.path.join(collection.dir, "endpoint.csv")
            source_csv_path = os.path.join(collection.dir, "source.csv")

            # Read endpoint and source CSV files
            endpoint_csv_df = pd.read_csv(endpoint_csv_path)
            source_csv_df = pd.read_csv(source_csv_path)

            # Get today's date in the format YYYY-MM-DD
            today_date = datetime.now().strftime("%Y-%m-%d")

            # Check if all endpoints and sources in collection_df_to_retire exist in CSV files
            missing_endpoints = collection_df_to_retire[
                ~collection_df_to_retire["endpoint"].isin(endpoint_csv_df["endpoint"])
            ]
            missing_sources = collection_df_to_retire[
                ~collection_df_to_retire["source"].isin(source_csv_df["source"])
            ]

            if not missing_endpoints.empty or not missing_sources.empty:
                print(
                    "Error: Some endpoints or sources were not found in the CSV files:"
                )
                if not missing_endpoints.empty:
                    print("Missing Endpoints:")
                    print(missing_endpoints)
                if not missing_sources.empty:
                    print("Missing Sources:")
                    print(missing_sources)
                return

            # Update end_date column in endpoint file
            endpoint_csv_df.loc[
                endpoint_csv_df["endpoint"].isin(collection_df_to_retire["endpoint"]),
                "end-date",
            ] = today_date

            # Update end_date column in source file
            source_csv_df.loc[
                source_csv_df["source"].isin(collection_df_to_retire["source"]),
                "end-date",
            ] = today_date

            # Write updated data back to CSV files
            endpoint_csv_df.to_csv(endpoint_csv_path, index=False)
            source_csv_df.to_csv(source_csv_path, index=False)

        except FileNotFoundError as e:
            print(f"Error: {e}. Please check the file paths and try again.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}.")
