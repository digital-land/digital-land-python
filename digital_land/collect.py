#!/usr/bin/env python3

#
#  collect resource
#
import os
from datetime import datetime
from timeit import default_timer as timer
from enum import Enum
import logging
import requests
import hashlib
import canonicaljson
import csv
from .adapter.file import FileAdapter


class FetchStatus(Enum):
    OK = 1
    EXPIRED = 2
    HASH_FAILURE = 3
    ALREADY_FETCHED = 4


class Collector:
    user_agent = "Digital Land data collector"
    resource_dir = "collection/resource/"
    log_dir = "collection/log/"

    def __init__(self):
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

    def get(self, url, log={}):
        logging.info("get %s" % url)

        try:
            start = timer()
            response = self.session.get(url, headers={"User-Agent": self.user_agent})
        except (
            requests.ConnectionError,
            requests.HTTPError,
            requests.Timeout,
            requests.TooManyRedirects,
            requests.exceptions.MissingSchema,
        ) as exception:
            logging.warning(exception)
            log["exception"] = type(exception).__name__
            response = None
        finally:
            log["elapsed"] = str(round(timer() - start, 3))

        content = None

        if response is not None:
            log["status"] = str(response.status_code)
            log["request-headers"] = dict(response.request.headers)
            log["response-headers"] = dict(response.headers)

            if log["status"] == "200" and not log["response-headers"].get(
                "Content-Type", ""
            ).startswith("text/html"):
                content = response.content

        return log, content

    def fetch(self, url, endpoint=None, log_datetime=datetime.utcnow(), end_date=""):
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
            "url": url,
            "datetime": log_datetime.isoformat(),
        }

        log, content = self.get(url, log)

        if content:
            log["resource"] = self.save_content(content)

        self.save_log(log_path, log)

        return FetchStatus.OK

    def collect(self, endpoint_path):
        for row in csv.DictReader(open(endpoint_path, newline="")):
            endpoint = row["endpoint"]
            url = row["endpoint-url"]

            # skip manually added files ..
            if not url or url.startswith("file:"):
                next

            self.fetch(url, endpoint=endpoint, end_date=row.get("end-date", ""))
