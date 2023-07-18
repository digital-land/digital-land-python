import logging
import os
import shutil
from collections import defaultdict

import pandas as pd
import urllib

from datetime import datetime
from pathlib import Path
from urllib.error import URLError

from digital_land.collection import Collection
from digital_land.commands import collect
from digital_land.schema import Schema
from digital_land.update import add_source_endpoint


def collection_add_source(entry, collection, endpoint_url, collection_dir):
    """
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


def clean_create_task_dirs(ctx):
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

    # clean up previous run data
    if organisation_dir.is_dir():
        shutil.rmtree(organisation_dir)
    if tmp_dir.is_dir():
        shutil.rmtree(tmp_dir)

    try:
        os.mkdir(organisation_dir)
        os.mkdir(tmp_dir)
        os.mkdir(datasource_log_dir)
        os.mkdir(log_dir)
    except OSError as exc:
        pass

    ctx.obj["PIPELINE_DIR"] = Path(ctx.obj["PIPELINE"].path)
    ctx.obj["DATASOURCE_LOG_DIR"] = datasource_log_dir
    ctx.obj["ISSUES_LOG_DIR"] = issues_log_dir
    ctx.obj["LOG_DIR"] = log_dir
    ctx.obj["ORGANISATION_DIR"] = organisation_dir
    ctx.obj["TMP_DIR"] = tmp_dir


def create_source_and_endpoint_entries(ctx):
    """
    appends entries to both source.csv and endpoints.csv
    :param ctx:
    :return:
    """

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


def collect_resources(ctx):
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
