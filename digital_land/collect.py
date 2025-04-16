#!/usr/bin/env python3

#
#  collect resource
#
import csv
import logging
import os
import re
from datetime import datetime
from enum import Enum
from timeit import default_timer as timer
from pathlib import Path

import canonicaljson
import requests

from .adapter.file import FileAdapter
from .plugins.sparql import get as sparql_get
from .plugins.wfs import get as wfs_get
from .plugins.arcgis import get as arcgis_get
import hashlib

logger = logging.getLogger(__name__)


def hash_file(path):
    """Returns the hash of a file as a hex digest."""
    # Read the file in 32MB chunks
    _chunk_size = 32 * 1024 * 1024
    hasher = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class FetchStatus(Enum):
    OK = 1
    EXPIRED = 2
    HASH_FAILURE = 3
    ALREADY_FETCHED = 4
    FAILED = 5


class Collector:
    user_agent = "MHCLG Planning Data Collector"
    resource_dir = "collection/resource/"
    log_dir = "collection/log/"

    def __init__(self, dataset="", collection_dir=None):

        self.dataset = dataset
        if collection_dir:
            self.resource_dir = Path(collection_dir) / "resource/"
            self.log_dir = Path(collection_dir) / "log/"
        self.session = requests.Session()
        self.session.mount("file:", FileAdapter())
        self.endpoint = {}

    def collection_dir_file_hashes(self, collection_dir=None):
        logger.setLevel(logging.INFO)
        if collection_dir:
            for csv_file in collection_dir.glob("*.csv"):
                if csv_file.is_file():
                    file_hash = hash_file(csv_file)
                    logger.info(f"CSV file: {csv_file.name} | hash: {file_hash}")
                else:
                    logger.info(f"Skipped non-file: {csv_file}")

    def url_endpoint(self, url):
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def log_path(self, log_datetime, endpoint):
        log_date = log_datetime.isoformat()[:10]
        return os.path.join(self.log_dir, log_date, endpoint + ".json")

    def save_log(self, path, log, refill_todays_logs=False):
        self.save(
            path,
            canonicaljson.encode_canonical_json(log),
            refill_todays_logs=refill_todays_logs,
        )

    def save_content(self, content):
        resource = hashlib.sha256(content).hexdigest()
        path = os.path.join(self.resource_dir, resource)
        self.save(path, content)
        return resource

    def save(self, path, data, refill_todays_logs=False):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # if refill_todays_logs=True then files in log_path need to be overwritten
        if not os.path.exists(path) or refill_todays_logs:
            logging.info(path)
            with open(path, "wb") as f:
                f.write(data)

    def strip_variable_content(self, content):
        # Define patterns for stripping timestamp and time
        strip_exps = [
            (re.compile(rb'"timeStamp"\s*:\s*"[^"]*"\s*,?'), rb""),
            (re.compile(rb'timeStamp="[^"]*" '), rb""),
            (re.compile(rb'"timeStamp":"[^"]*",'), rb""),
        ]

        for strip_exp, replacement in strip_exps:
            content = strip_exp.sub(replacement, content)

        # Clean up any trailing commas in the JSON content
        content = re.sub(rb",\s*}", b"}", content)

        return content

    def get(self, url, log={}, verify_ssl=True, plugin="get"):
        logging.info("%s %s" % (plugin, url))
        log["ssl-verify"] = verify_ssl

        try:
            response = self.session.get(
                url,
                headers={"User-Agent": self.user_agent},
                timeout=120,
                verify=verify_ssl,
            )
        except (requests.RequestException,) as exception:
            logging.warning(exception)
            log["exception"] = type(exception).__name__
            response = None

        content = None

        if response is not None:
            log["status"] = str(response.status_code)
            log["request-headers"] = dict(response.request.headers)
            log["response-headers"] = dict(response.headers)
            if log["status"] == "200" and not response.headers.get(
                "Content-Type", ""
            ).startswith("text/html"):
                content = response.content
                # Apply timestamp stripping for JSON/GeoJSON/XML formats
                content_type = (
                    response.headers.get("Content-Type", "").lower().replace(" ", "")
                )
                if content_type.startswith(
                    ("application/json", "application/geo+json", "application/xml")
                ):
                    content = self.strip_variable_content(content)
        return log, content

    def fetch(
        self,
        url,
        endpoint=None,
        log_datetime=datetime.utcnow(),
        end_date="",
        plugin="",
        refill_todays_logs=False,
    ):
        if end_date and datetime.strptime(end_date, "%Y-%m-%d") < log_datetime:
            return FetchStatus.EXPIRED

        url_endpoint = self.url_endpoint(url)
        if not endpoint:
            endpoint = url_endpoint
        elif endpoint != url_endpoint:
            logging.error(
                "url '%s' given endpoint %s expected %s" % (url, endpoint, url_endpoint)
            )
            return FetchStatus.HASH_FAILURE

        # fetch each source at most once per-day, though with an option to re-collect the latest day's sources
        log_path = self.log_path(log_datetime, endpoint)
        if not refill_todays_logs:
            if os.path.isfile(log_path):
                logging.debug(f"{log_path} exists")
                return FetchStatus.ALREADY_FETCHED

        log = {
            "endpoint-url": url,
            "entry-date": log_datetime.isoformat(),
        }

        start = timer()

        # TBD: use pluggy and move modules to digital-land.plugin.xxx namespace?
        if not plugin:
            log, content = self.get(url, log)
        elif plugin == "arcgis":
            log, content = arcgis_get(self, url, log)
        elif plugin == "wfs":
            log, content = wfs_get(self, url, log)
        elif plugin == "sparql":
            log, content = sparql_get(self, url, log)
        else:
            logging.error("unknown plugin '%s' for endpoint %s" % (plugin, endpoint))

        log["elapsed"] = str(round(timer() - start, 3))

        status = self.save_resource(content, log_path, log)
        self.save_log(log_path, log, refill_todays_logs=refill_todays_logs)
        return status

    def save_resource(self, content, url, log):
        if content:
            try:
                log["resource"] = self.save_content(content)
                return FetchStatus.OK
            except Exception as exception:
                logging.warning(f"Failed to save data from '{url} ({exception})")
                log["exception"] = type(exception).__name__

        return FetchStatus.FAILED

    def collect(self, endpoint_path, refill_todays_logs=False):
        for row in csv.DictReader(open(endpoint_path, newline="")):
            endpoint = row["endpoint"]
            url = row["endpoint-url"]
            plugin = row.get("plugin", "")

            # skip manually added files ..
            if not url:
                continue

            self.fetch(
                url,
                endpoint=endpoint,
                end_date=row.get("end-date", ""),
                plugin=plugin,
                refill_todays_logs=refill_todays_logs,
            )
