import os
import os.path
import re
import glob
from datetime import datetime
import validators
import logging
import hashlib
import csv
import json
import canonicaljson


def parse_log_path(path):
    m = re.match(r"^.*\/([-\d]+)\/(\w+).json", path)
    return m.groups()


def parse_resource_path(path):
    m = re.match(r"^.*\/(\w+)$", path)
    return m.groups()[0]


def parse_json_path(path):
    m = re.match(r"^.*\/(\w+).json", path)
    return m.groups()[0]


def valid_url(n, url):
    if url != "" and not validators.url(url):
        logging.error("line %d: invalid url %s" % (n, url))


def valid_date(n, date):
    if (
        date is not None
        and date != ""
        and date != datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    ):
        logging.error("line %d: invalid date %s" % (n, date))


def save_json(path, data):
    logging.info(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def csv_writer(path, fieldnames):
    logging.info(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = csv.DictWriter(
        open(path, "w"), fieldnames=fieldnames, extrasaction="ignore"
    )
    writer.writeheader()
    return writer


def count_rows_in(path):
    try:
        with open(path) as f:
            reader = csv.reader(f)
            return sum(1 for row in reader) - 1  # Subtract header
    except UnicodeDecodeError:
        return -1


class Indexer:
    #
    #  create an index for a collection
    #

    # dataset_dir = "dataset/"
    log_dir = "collection/log/"
    resource_dir = "collection/resource/"
    endpoint_path = "collection/endpoint.csv"
    # validation_dir = "validation/"
    index_dir = "index/"
    idx = {}
    resources = {}
    prune = False

    def __init__(self, log_dir=None):
        if log_dir:
            self.log_dir = log_dir

    def save_csv(self, name, fieldnames, data):
        path = os.path.join(self.index_dir, name + ".csv")
        keyfield = fieldnames[0]
        writer = csv_writer(path, fieldnames)
        for key in sorted(data):
            row = data[key]
            if keyfield not in row:
                row[keyfield] = key
            writer.writerow(row)

    #     def load(dataset):
    #         n = 0
    #         ncols = 0
    #         for row in csv.reader(open(os.path.join(dataset_dir, dataset + ".csv"))):
    #             if not n:
    #                 ncols = len(row)
    #                 n = 1
    #             else:
    #                 n += 1
    #                 if ncols != len(row):
    #                     logging.error(
    #                         "line %d: %d columns instead of %d" % (n, len(row), ncols)
    #                     )

    #         n = 1

    def load_endpoint(self):
        n = 1
        for row in csv.DictReader(open(self.endpoint_path)):
            n += 1
            valid_url(n, row["endpoint-url"])
            valid_date(n, row["start-date"])
            valid_date(n, row["end-date"])

            key = hashlib.sha256(row["endpoint-url"].encode("utf-8")).hexdigest()
            self.idx.setdefault(
                key, {"url": row.get("endpoint-url", ""), "log": {}, "organisation": {}}
            )
            self.idx[key]["organisation"].setdefault(row["organisation"], {})
            for field in [
                # "documentation-url",
                "start-date",
                "end-date",
            ]:
                if row.get(field, ""):
                    self.idx[key]["organisation"][row["organisation"]][field] = row[
                        field
                    ]

    def add(self, path, date, key, h):
        if not h.get("endpoint-url", ""):
            logging.error("no url in %s" % (path))

        # check key in log filename matches url
        _key = hashlib.sha256(h["endpoint-url"].encode("utf-8")).hexdigest()
        if key != _key:
            logging.warning(
                "incorrect key for %s expected %s in %s"
                % (h["endpoint-url"], _key, path)
            )
            key = _key

        e = {}
        for field in ["status", "exception", "entry-date", "elapsed", "resource"]:
            if field in h and h[field]:
                e[field] = h[field]

        if h.get("status", "") == "200" and "response-headers" in h:
            for field in ["Content-Type", "Content-Length"]:
                if field in h["response-headers"]:
                    e[field] = h["response-headers"][field]

        if "resource" in e:
            self.resources.setdefault(e["resource"], [])
            self.resources[e["resource"]].append(path)

        if key not in self.idx:
            logging.error(
                "no dataset entry for: %s %s cited in %s"
                % (h["endpoint-url"], key, path)
            )
            self.idx.setdefault(key, {"url": h["endpoint-url"], "log": {}})

        # avoid date collisions with a valid key
        while date in self.idx[key]["log"]:
            date += "⚠"

        if self.prune:
            for organisation in self.idx[key]["organisation"]:
                end_date = self.idx[key]["organisation"][organisation].get(
                    "end-date", None
                )
                if end_date and datetime.strptime(
                    end_date, "%Y-%m-%d"
                ) < datetime.strptime(date, "%Y-%m-%d"):
                    logging.warning(
                        "collection after %s end-date %s for %s: %s"
                        % (organisation, end_date, h["endpoint-url"], path)
                    )
        self.idx[key]["log"][date] = e

    def index(self):
        # logging.basicConfig(
        #     level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        # )

        # load datasets
        # for dataset in argv[1:]:
        #     load(dataset)

        self.load_endpoint()

        # process log files
        for path in glob.glob("%s*/*.json" % (self.log_dir)):
            (date, key) = parse_log_path(path)
            h = json.load(open(path))
            self.add(path, date, key, h)

        # check resource files are in the log
        for path in glob.glob("%s*" % (self.resource_dir)):
            resource = parse_resource_path(path)
            if resource in self.resources:
                self.resources[resource] = None
            else:
                logging.error("no log for %s" % (path))

        # check resources in the log exist as files
        for resource in self.resources:
            if self.resources[resource]:
                logging.error(
                    "missing resource: %s listed in %s"
                    % (resource, ", ".join(self.resources[resource]))
                )
            # else:
            #     self.resources[resource] = {
            #         "row-count": count_rows_in(}

        # for path in glob.glob("%s*.json" % (self.resource_dir)):
        for entry in os.scandir(self.resource_dir):
            resource = parse_resource_path(entry.path)
            if resource not in self.resources:
                logging.error("no log for %s" % (entry.path))

            self.resources[resource] = {"row-count": count_rows_in(entry.path)}

        # resources[resource] = {
        #     "media-type": v["meta_data"].get("media_type", ""),
        #     "suffix": v["meta_data"].get("suffix", ""),
        #     "valid": v["result"].get("valid", False),
        #     "error-count": v["result"].get("error-count", -1),
        #     "row-count": v["result"]["tables"][0].get("row-count", 0),
        # }

        # process validation
        # for path in glob.glob("%s*.json" % (validation_dir)):
        #     v = json.load(open(path))
        #     resource = parse_json_path(path)
        #     if resource not in resources:
        #         logging.error("no log for %s" % (path))

        #     if not os.path.isfile(os.path.join(resource_dir, resource)):
        #         logging.error("no resource file for %s" % (path))

        #     resources[resource] = {
        #         "media-type": v["meta_data"].get("media_type", ""),
        #         "suffix": v["meta_data"].get("suffix", ""),
        #         "valid": v["result"].get("valid", False),
        #         "error-count": v["result"].get("error-count", -1),
        #         "row-count": v["result"]["tables"][0].get("row-count", 0),
        #     }

        for resource, r in self.resources.items():
            if not r or "row-count" not in r:
                logging.error("%s missing" % resource)
                self.resources[resource] = {}
            # if not r or "valid" not in r:
            #     logging.error("%s%s.json missing" % (validation_dir, resource))

        # save as single JSON file
        save_json(
            "index/index.json",
            canonicaljson.encode_canonical_json(
                {"key": self.idx, "resource": self.resources}
            ),
        )

        # save index CSV files
        self.save_csv(
            "resource",
            ["resource", "row-count"],
            self.resources,
        )
        self.save_csv("link", ["link", "url"], self.idx)

        log = {}
        last_date = "0"
        for link in self.idx:
            for date, entry in self.idx[link]["log"].items():
                entry = {key.lower(): value for key, value in entry.items()}
                entry["link"] = link
                entry["date"] = date
                log["%s-%s" % (date, link)] = entry
                if entry["date"] > last_date:
                    last_date = entry["date"]

        self.save_csv(
            "log",
            [
                "entry-date",
                "link",
                "status",
                "elapsed",
                "resource",
                "content-type",
                "content-length",
            ],
            log,
        )

        link_resource = {}
        for entry in log.values():
            if "resource" in entry:
                link_resource[entry["link"] + entry["resource"]] = {
                    "link": entry["link"],
                    "resource": entry["resource"],
                }
                date = entry["date"]
                if (
                    "start-date" not in self.resources[entry["resource"]]
                    or self.resources[entry["resource"]]["start-date"] > date
                ):
                    self.resources[entry["resource"]]["start-date"] = date
                if (
                    "end-date" not in self.resources[entry["resource"]]
                    or self.resources[entry["resource"]]["end-date"] < date
                ):
                    self.resources[entry["resource"]]["end-date"] = date

        for resource in self.resources:
            if self.resources[resource].get("end-date", None) == last_date:
                self.resources[resource]["end-date"] = ""

        self.save_csv("link-resource", ["link", "resource"], link_resource)

        rows = {}
        for link, entry in self.idx.items():
            for organisation in entry["organisation"]:
                rows[link + organisation] = {
                    "link": link,
                    "organisation": organisation,
                }
        self.save_csv("link-organisation", ["link", "organisation"], rows)

        # index for harmonising missing OrganisationURI values
        rows = {}
        for k, entry in link_resource.items():
            link = entry["link"]
            for organisation in self.idx[link]["organisation"]:
                rows[entry["resource"] + organisation] = {
                    "resource": entry["resource"],
                    "organisation": organisation,
                    "start-date": self.resources[entry["resource"]]["start-date"],
                    "end-date": self.resources[entry["resource"]]["end-date"],
                }
        self.save_csv(
            "resource-organisation",
            ["resource", "organisation", "start-date", "end-date"],
            rows,
        )
