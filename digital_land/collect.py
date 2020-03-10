#!/usr/bin/env python3

#
#  collect resource
#
from sys import argv
import os
from datetime import datetime
from timeit import default_timer as timer
import logging
import requests
import hashlib
import canonicaljson
import csv

user_agent = "Digital Land data collector"
resource_dir = "collection/resource/"
log_dir = "collection/log/"
dataset_dir = "dataset/"


def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        logging.info(path)
        with open(path, "wb") as f:
            f.write(data)


def fetch(dataset, organisation, url, end_date):
    if not url:
        return

    if end_date and datetime.strptime(end_date, "%Y-%m-%d") < datetime.now():
        return

    headers = {
        "dataset": dataset,
        "organisation": organisation,
        "url": url,
        "datetime": datetime.utcnow().isoformat(),
    }

    entry = hashlib.sha256(url.encode("utf-8")).hexdigest()
    log_path = os.path.join(log_dir, headers["datetime"][:10], entry + ".json")

    if os.path.isfile(log_path):
        return

    logging.info(" ".join([dataset, organisation, url]))

    try:
        start = timer()
        response = requests.get(url, headers={"User-Agent": user_agent})
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
            save(os.path.join(resource_dir, resource), response.content)

    log_json = canonicaljson.encode_canonical_json(headers)
    save(log_path, log_json)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    for dataset in argv[1:]:
        for row in csv.DictReader(open(os.path.join(dataset_dir, dataset + ".csv"))):
            fetch(dataset, row["organisation"], row["resource-url"], row["end-date"])
