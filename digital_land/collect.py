#!/usr/bin/env python3

#
#  collect resource
#
import os
from datetime import datetime
from timeit import default_timer as timer
import logging
import requests
import hashlib
import canonicaljson
import csv
from .adapter.file import FileAdapter


class Collector:
    user_agent = "Digital Land data collector"
    resource_dir = "collection/resource/"
    log_dir = "collection/log/"

    def __init__(self):
        self.session = requests.Session()
        self.session.mount("file:", FileAdapter())

    def log_path(self, log_datetime, url):
        log_date = log_datetime.isoformat()[:10]
        source = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return os.path.join(self.log_dir, log_date, source + ".json")

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

    def fetch(self, url, log_datetime=datetime.utcnow(), end_date=""):
        if end_date and datetime.strptime(end_date, "%Y-%m-%d") < log_datetime:
            return

        # fetch each source at most once per-day
        log_path = self.log_path(log_datetime, url)
        if os.path.isfile(log_path):
            return

        log = {
            "url": url,
            "datetime": log_datetime.isoformat(),
        }

        log, content = self.get(url, log)

        if content:
            log["resource"] = self.save_content(content)

        self.save_log(log_path, log)

    def collect(self, path):
        for row in csv.DictReader(open(path, newline="")):
            url = row["resource-url"]

            # skip manually added files ..
            if url and not url.startswith("file:"):
                return

            self.fetch(url, end_date=row["end-date"])
