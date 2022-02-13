#!/usr/bin/env python3

#
#  collect resource
#
import csv
import hashlib
import logging
import os
from datetime import datetime
from enum import Enum
from timeit import default_timer as timer

import canonicaljson
import requests

from .adapter.file import FileAdapter
from .plugins.sparql import get as sparql_get
from .plugins.wfs import get as wfs_get


class FetchStatus(Enum):
    OK = 1
    EXPIRED = 2
    HASH_FAILURE = 3
    ALREADY_FETCHED = 4
    FAILED = 5


class Collector:
    user_agent = "DLUHC Digital Land"
    resource_dir = "collection/resource/"
    log_dir = "collection/log/"

    def __init__(self, dataset="", collection_dir=None):
        self.dataset = dataset
        if collection_dir:
            self.resource_dir = collection_dir / "resource/"
            self.log_dir = collection_dir / "log/"
        self.session = requests.Session()
        self.session.mount("file:", FileAdapter())
        self.endpoint = {}

    def url_endpoint(self, url):
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def log_path(self, log_datetime, endpoint):
        log_date = log_datetime.isoformat()[:10]
        return os.path.join(self.log_dir, log_date, endpoint + ".json")

    def save_log(self, path, log):
        self.save(path, canonicaljson.encode_canonical_json(log))

    def save_content(self, content):
        resource = hashlib.sha256(content).hexdigest()
        path = os.path.join(self.resource_dir, resource)
        self.save(path, content)
        return resource

    def save(self, path, data):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            logging.info(path)
            with open(path, "wb") as f:
                f.write(data)

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
        except requests.exceptions.SSLError:
            logging.warning("Retrying without certificate validation due to SSLError")
            return self.get(url, log, False)
        except (
            requests.ConnectionError,
            requests.HTTPError,
            requests.Timeout,
            requests.TooManyRedirects,
            requests.exceptions.MissingSchema,
            requests.exceptions.ChunkedEncodingError,
        ) as exception:
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

        return log, content

    def fetch(
        self,
        url,
        endpoint=None,
        log_datetime=datetime.utcnow(),
        end_date="",
        plugin="",
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

        # fetch each source at most once per-day
        log_path = self.log_path(log_datetime, endpoint)
        if os.path.isfile(log_path):
            return FetchStatus.ALREADY_FETCHED

        log = {
            "endpoint-url": url,
            "entry-date": log_datetime.isoformat(),
        }

        start = timer()

        # TBD: use pluggy and move modules to digital-land.plugin.xxx namespace?
        if plugin == "":
            log, content = self.get(url, log)
        elif plugin == "wfs":
            log, content = wfs_get(self, url, log)
        elif plugin == "sparql":
            log, content = sparql_get(self, url, log)
        else:
            logging.error("unknown plugin '%s' for endpoint %s" % plugin, endpoint)

        log["elapsed"] = str(round(timer() - start, 3))

        if content:
            status = FetchStatus.OK
            log["resource"] = self.save_content(content)
        else:
            status = FetchStatus.FAILED

        self.save_log(log_path, log)
        return status

    def collect(self, endpoint_path):
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
            )
