import logging
import os
import shutil
import pandas as pd
import urllib

from collections import defaultdict
from datetime import datetime
from digital_land.collection import Collection, ResourceLogStore
from digital_land.commands import collect
from digital_land.phase.phase import Phase
from digital_land.schema import Schema
from digital_land.store.item import ItemStore
from digital_land.update import add_source_endpoint
from digital_land.register import hash_value
from digital_land.phase.lookup import key
from pathlib import Path
from urllib.error import URLError


# =====================================================================
# Main Process Tasks
# =====================================================================
def task_preprocess(ctx):
    """
    preparatory steps to tidy up previous runs,
    and populate the context
    :param ctx:
    :return:
    """

    collection_dir = ctx.obj["COLLECTION_DIR"]
    root_coll_dir = collection_dir.parent
    datasource_log_dir = root_coll_dir / "log"
    issues_log_dir = root_coll_dir / "log"
    log_dir = collection_dir / "log"
    organisation_dir = root_coll_dir / "organisation"
    tmp_dir = root_coll_dir / "tmp"
    collection_resource_dir = collection_dir / "resource"

    ctx.obj["PIPELINE_DIR"] = Path(ctx.obj["PIPELINE"].path)
    ctx.obj["DATASOURCE_LOG_DIR"] = datasource_log_dir
    ctx.obj["ISSUES_LOG_DIR"] = issues_log_dir
    ctx.obj["LOG_DIR"] = log_dir
    ctx.obj["ORGANISATION_DIR"] = organisation_dir
    ctx.obj["TMP_DIR"] = tmp_dir
    ctx.obj["COLLECTION_RESOURCE_DIR"] = collection_resource_dir

    # clean up previous run data
    preprocess_collection_csvs(ctx)

    if log_dir.is_dir():
        shutil.rmtree(log_dir)
    if organisation_dir.is_dir():
        shutil.rmtree(organisation_dir)

    try:
        os.mkdir(log_dir)
        os.mkdir(organisation_dir)
        os.mkdir(tmp_dir)
    except OSError as exc:
        pass


def task_create_source_and_endpoint_entries(ctx):
    """
    appends entries to both source.csv and endpoints.csv from
    the records in the csv at csv_file_path
    :param ctx:
    :return:
    """

    # read and process each record in the csv at csv_file_path
    csv_file_path = ctx.obj["CSV_FILE_PATH"]

    cols = [
        "dataset",
        "organisation",
        "name",
        "documentation-url",
        "endpoint-url",
        "start-date",
    ]
    csv_file_df = pd.read_csv(
        csv_file_path,
        header=0,
        index_col=False,
        usecols=cols,
    )

    collection = Collection()
    ctx.obj["COLLECTION"] = collection

    for idx, row in csv_file_df.iterrows():
        param_dataset = row["dataset"]
        param_endpoint_url = row["endpoint-url"]
        param_documentation_url = row["documentation-url"]
        param_organisation_name = row["organisation"]
        param_reference = row["name"]
        param_start_date = row["start-date"]

        if not ctx.obj["PIPELINE"].name:
            ctx.obj["PIPELINE"].name = param_dataset

        # do not process if important fields are empty or missing
        if (not param_organisation_name) or (type(param_organisation_name) is float):
            continue
        if (not param_reference) or (type(param_reference) is float):
            continue
        if (not param_endpoint_url) or (type(param_endpoint_url) is float):
            continue

        # handle different date entry formats
        str_date_fmt = ""
        if type(param_start_date) is datetime:
            str_date_fmt = param_start_date.strftime("%Y-%m-%d")
        elif type(param_start_date) is int:
            param_start_date_str = str(param_start_date)
            param_start_date_dt = datetime.strptime(param_start_date_str, "%Y%m%d")
            if type(param_start_date_dt) is datetime:
                str_date_fmt = param_start_date_dt.strftime("%Y-%m-%d")
        else:
            str_date_fmt = pd.Timestamp.now().strftime("%Y-%m-%d")

        # prepare row for processing
        entry_ctx = [
            "collection", param_dataset,
            "organisation", param_organisation_name,
            "licence", param_reference,
            "attribution", param_reference,
            "documentation-url", param_documentation_url,
            "start-date", str_date_fmt,
            "end-date", "",
            "pipelines", param_dataset,
        ]
        entry = defaultdict(
            str,
            {entry_ctx[i]: entry_ctx[i + 1] for i in range(0, len(entry_ctx), 2)},
        )

        # process row
        collection_dir = ctx.obj["COLLECTION_DIR"].absolute()
        if collection.name is None:
            collection.name = param_dataset
            collection.directory = collection_dir
            collection.load()

        if type(param_endpoint_url) == str:
            collection_add_source(
                entry, collection, param_endpoint_url, collection_dir
            )

        print("")
        print(f"           param_dataset:{param_dataset}")
        print(f"      param_endpoint_url:{param_endpoint_url}")
        print(f" param_documentation_url:{param_documentation_url}")
        print(f" param_organisation_name:{param_organisation_name}")
        print(f"         param_reference:{param_reference}")
        print(f"        param_start_date:{param_start_date}")


def task_collect_resources(ctx):
    """
    using existing functionality to create a collector and retrieve remote resources
    :param ctx:
    :return:
    """
    endpoint_path = ctx.obj["COLLECTION_DIR"] / "endpoint.csv"
    collection_dir_str = ctx.obj["COLLECTION_DIR"].absolute()
    pipeline = ctx.obj["PIPELINE"]

    collect(
        endpoint_path,
        collection_dir_str,
        pipeline,
    )


def task_populate_resource_and_log_csvs(ctx):
    collection = ctx.obj["COLLECTION"]
    collection_dir = ctx.obj["COLLECTION_DIR"]

    logs_path = collection_dir / "log/*/"
    collection = load_log_items(collection, log_directory=logs_path)

    collection.log.save_csv(directory=collection.directory)
    collection.resource.save_csv(directory=collection.directory)


def task_postprocess(ctx):
    postprocess_collection_csvs(ctx)


# =====================================================================
# Supporting functions and classes
# =====================================================================
def collection_add_source(entry, collection, endpoint_url, collection_dir):
    """
    creates entries in source and endpoint csvs, using the info in entry
    followed by a sequence of optional name and value pairs including the following names:
    "attribution", "licence", "pipelines", "status", "plugin",
    "parameters", "start-date", "end-date"
    """
    entry["collection"] = collection.name
    entry["endpoint-url"] = endpoint_url
    allowed_names = set(
        list(Schema("endpoint").fieldnames) + list(Schema("source").fieldnames)
    )

    for key in entry.keys():
        if key not in allowed_names:
            logging.error(f"unrecognised argument '{key}'")
            continue

    add_source_endpoint(entry, directory=collection_dir, collection=collection)


def preprocess_collection_csvs(ctx):
    """
    backup existing source.csv & endpoint.csv and remove entries in each.
    starting from clean files reduces the number of collections performed
    :return:
    """

    source_csv_path = ctx.obj["COLLECTION_DIR"] / "source.csv"
    source_csv_backup_path = ctx.obj["COLLECTION_DIR"] / "source.csv.bak"
    endpoint_csv_path = ctx.obj["COLLECTION_DIR"] / "endpoint.csv"
    endpoint_csv_backup_path = ctx.obj["COLLECTION_DIR"] / "endpoint.csv.bak"

    # remove existing backups
    for file in [source_csv_backup_path, endpoint_csv_backup_path]:
        try:
            os.remove(file)
        except FileNotFoundError:
            continue

    # create new backups
    if source_csv_path.is_file():
        shutil.copyfile(source_csv_path, source_csv_backup_path)

    if endpoint_csv_path.is_file():
        shutil.copyfile(endpoint_csv_path, endpoint_csv_backup_path)

    # create empty source and endpoint csvs
    with open(source_csv_path, "r") as file:
        source_csv_header = file.readline()
    with open(endpoint_csv_path, "r") as file:
        endpoint_csv_header = file.readline()

    with open(source_csv_path, "w") as file:
        file.write(f"{source_csv_header}\n")
    with open(endpoint_csv_path, "w") as file:
        file.write(f"{endpoint_csv_header}\n")


def postprocess_collection_csvs(ctx):
    """
    clean up files and folders that do not get typically get checked in to
    source control.
    NOTE: by-pass this routine if you wish to inspect the final process output
    files
    :param ctx:
    :return:
    """
    tmp_dir = ctx.obj["TMP_DIR"]
    collection_resource_dir = ctx.obj["COLLECTION_RESOURCE_DIR"]

    if tmp_dir.is_dir():
        shutil.rmtree(tmp_dir)
    if collection_resource_dir.is_dir():
        shutil.rmtree(collection_resource_dir)

    # TODO
    print(">>> TODO: postprocess_collection_csvs")


def load_log_items(collection: Collection, directory=None, log_directory=None):
    collection.log = CollectionLogStore(Schema("log"))
    collection.log.load(directory=log_directory)

    collection.resource = ResourceLogStore(Schema("resource"))
    collection.resource.load(
        log=collection.log, source=collection.source, directory=directory
    )

    return collection


class CollectionLogStore(ItemStore):

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

        # print(item["url"], hash_value(item["url"]))
        if endpoint != item["endpoint"]:
            logging.warning(
                "incorrect endpoint in path %s expected %s" % (path, item["endpoint"])
            )


class EntityNumGen:
    def __init__(self, entity_num_state: dict = None):
        self.entity_num_state = entity_num_state

    def next(self):
        current = self.entity_num_state["current"]
        new_current = current + 1

        if new_current > self.entity_num_state["range_max"]:
            new_current = self.entity_num_state["range_min"]

        if new_current < self.entity_num_state["range_min"]:
            new_current = self.entity_num_state["range_min"]

        self.entity_num_state["current"] = new_current

        return new_current


# print all lookups that aren't found will need to read through all files
class PrintLookupPhase(Phase):
    def __init__(self, lookups={}, entity_num_gen=None):
        self.lookups = lookups
        self.entity_field = "entity"
        self.entity_num_gen = entity_num_gen

    def lookup(self, **kwargs):
        return self.lookups.get(key(**kwargs), "")

    def process(self, stream):
        for block in stream:
            row = block["row"]
            entry_number = block["entry-number"]
            prefix = row.get("prefix", "")
            reference = row.get("reference", "")
            organisation = row.get("organisation", "")
            if prefix:
                if not row.get(self.entity_field, ""):
                    entity = (
                        # by the resource and row number
                            (
                                    self.entity_field == "entity"
                                    and self.lookup(prefix=prefix, entry_number=entry_number)
                            )
                            # TBD: fixup prefixes so this isn't needed ..
                            # or by the organisation and the reference
                            or self.lookup(
                                prefix=prefix,
                                organisation=organisation,
                                reference=reference,
                            )
                    )

            if not entity:
                if self.entity_num_gen:
                    print(f"{prefix},,{organisation},{reference},{self.entity_num_gen.next()}")
                else:
                    print(f"{prefix},,{organisation},{reference}")

            yield block

