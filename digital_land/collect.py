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


class Collector:
    user_agent = "Digital Land data collector"
    resource_dir = "collection/resource/"
    log_dir = "collection/log/"

    def save(self, path, data):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            logging.info(path)
            with open(path, "wb") as f:
                f.write(data)

    def fetch(self, url, end_date=""):
        if not url:
            return

        if end_date and datetime.strptime(end_date, "%Y-%m-%d") < datetime.now():
            return

        headers = {
            "url": url,
            "datetime": datetime.utcnow().isoformat(),
        }

        entry = hashlib.sha256(url.encode("utf-8")).hexdigest()
        log_path = os.path.join(self.log_dir, headers["datetime"][:10], entry + ".json")

        if os.path.isfile(log_path):
            return

        logging.info(" ".join([url]))

        try:
            start = timer()
            response = requests.get(url, headers={"User-Agent": self.user_agent})
        except (
            requests.ConnectionError,
            requests.HTTPError,
            requests.Timeout,
            requests.TooManyRedirects,
            requests.exceptions.MissingSchema,
        ) as exception:
            logging.warning(exception)
            headers["exception"] = type(exception).__name__
            response = None
        finally:
            headers["elapsed"] = str(round(timer() - start, 3))

        if response is not None:
            headers["status"] = str(response.status_code)
            headers["request-headers"] = dict(response.request.headers)
            headers["response-headers"] = dict(response.headers)

            if headers["status"] == "200" and not response.headers[
                "Content-Type"
            ].startswith("text/html"):
                resource = hashlib.sha256(response.content).hexdigest()
                headers["resource"] = resource
                self.save(os.path.join(self.resource_dir, resource), response.content)

        log_json = canonicaljson.encode_canonical_json(headers)
        self.save(log_path, log_json)

    def collect(self, path):
        for row in csv.DictReader(open(path, newline="")):
            self.fetch(row["resource-url"], row["end-date"])
