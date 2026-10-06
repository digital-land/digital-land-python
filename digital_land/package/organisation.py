import os
import csv
import sys
import json
import requests
import logging
from pathlib import Path

from .csv import CsvPackage

logger = logging.getLogger(__name__)


def organisation_datasets(rows, environment=None):
    """The organisation datasets to combine into organisation.csv.

    rows are the specification's dataset.csv rows. A dataset is used when it has
    the organisation typology, a collection and no end-date. When environment is
    set it must also be built there, using the same rules as
    is_dataset_available in airflow-dags (dags/utils.py): production everywhere,
    staging in staging and development, development only in development, and a
    blank environment nowhere.
    """
    available = {"production"}
    if environment in ("staging", "development"):
        available.add("staging")
    if environment == "development":
        available.add("development")

    return [
        row["dataset"]
        for row in rows
        if row["typology"] == "organisation"
        and row["collection"]
        and not row["end-date"]
        and (not environment or row.get("environment", "") in available)
    ]


def load_lpas(path):
    lpas = {}
    for row in csv.DictReader(open(path)):
        lpas[row["reference"]] = row
    return lpas


def load_organisations(path):
    organisations = {}
    for row in csv.DictReader(open(path)):
        curie = row.get("organisation", "")
        if not curie:
            curie = f'{row["prefix"]}:{row["reference"]}'
        organisations.setdefault(curie, {})
        for field, value in row.items():
            if value:
                organisations[curie][field] = value
    return organisations


def issue(severity, row, issue, field="", value=""):
    line = {
        "datapackage": "organisation",
        "entity": row["entity"],
        "prefix": row["prefix"],
        "reference": row["reference"],
        "severity": severity,
        "issue": issue,
        "field": field,
        "value": value,
    }
    if severity in ["critical", "error"]:
        print(
            f'{line["severity"]} {line["prefix"]}:{line["reference"]} {issue} {field} {value}',
            file=sys.stderr,
        )
    return line


def save_issues(issues, path):
    fieldnames = [
        "datapackage",
        "severity",
        "entity",
        "prefix",
        "reference",
        "issue",
        "field",
        "value",
    ]
    w = csv.DictWriter(open(path, "w"), fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for row in issues:
        w.writerow(row)


class OrganisationPackage(CsvPackage):
    def __init__(self, **kwargs):
        self.flattened_dir = kwargs.pop("flattened_dir", None)
        self.dataset_dir = kwargs.pop("dataset_dir", None)
        self.download_url = kwargs.pop("download_url", None)
        self.cache_dir = kwargs.pop("cache_dir", None)
        self.environment = kwargs.pop("environment", None)
        if self.download_url and self.download_url[-1] != "/":
            self.download_url += "/"
        super().__init__("organisation", tables={"organisation": None}, **kwargs)

    def create(self):
        if self.download_url:
            self.fetch_dataset()
            self.dataset_dir = self.cache_dir
            return self.create_from_dataset()

        if self.dataset_dir:
            return self.create_from_dataset()

        if self.flattened_dir:
            return self.create_from_flattened()

        raise RuntimeError(
            "One of download-url, dataset-dir or flatteneed-dir must be specified"
        )

    def source_filenames(self):
        with open(
            os.path.join(self.specification.specification_dir, "dataset.csv"),
            newline="",
        ) as f:
            datasets = organisation_datasets(csv.DictReader(f), self.environment)
        logger.info(f"combining organisation datasets: {datasets}")
        return [f"{dataset}.csv" for dataset in datasets]

    def create_from_flattened(self):
        # get field names
        org_field_names = self.specification.schema["organisation"]["fields"]

        orgs = []
        for file in self.source_filenames():
            filepath = Path(self.flattened_dir) / file

            if not os.path.exists(filepath):
                logger.warning(f"{filepath} not found.")
                continue

            with open(filepath, newline="") as f:
                for row in csv.DictReader(f):
                    # hack to replace "_" with "-" in fieldnames
                    if row["typology"] == "organisation":
                        row = {k.replace("_", "-"): v for k, v in row.items()}
                        if not row.get("organisation", None):
                            row["organisation"] = (
                                row["dataset"] + ":" + row["reference"]
                            )
                        org = {k: v for k, v in row.items() if k in org_field_names}
                        orgs.append(org)

        self.write(org_field_names, orgs)

    def create_from_dataset(self):
        # get field names
        org_field_names = self.specification.schema["organisation"]["fields"]

        orgs = []
        for file in self.source_filenames():
            filepath = Path(self.dataset_dir) / file

            if not os.path.exists(filepath):
                logger.warning(f"{filepath} not found.")
                continue

            with open(filepath, newline="") as f:
                for row in csv.DictReader(f):
                    # Only process rows with an organisation_entity
                    if "organisation_entity" in row:
                        if row["typology"] == "organisation":
                            row = {k.replace("_", "-"): v for k, v in row.items()}
                            if not row.get("organisation", None):
                                row["organisation"] = (
                                    row["dataset"] + ":" + row["reference"]
                                )

                            # Add in the stuff rom the JSON
                            for k, v in json.loads(row["json"]).items():
                                row[k] = v

                            orgs.append(
                                {k: v for k, v in row.items() if k in org_field_names}
                            )

        self.write(org_field_names, orgs)

    def check(self, lpa_path, output_path):
        lpas = load_lpas(lpa_path)
        organisations = load_organisations(self.path)

        entities = {}
        wikidatas = {}
        bas = {}
        odcs = {}
        issues = []

        for organisation, row in organisations.items():

            # look for duplicate entities
            if row["entity"] in entities:
                issues.append(issue("error", row, "duplicate entity"))
            else:
                entities[row["entity"]] = organisation

            # check wikidata
            wikidata = row.get("wikidata", "")
            if wikidata and wikidata in wikidatas:
                severity = "warning" if row["entity"] in ["600001"] else "error"
                issues.append(
                    issue(
                        severity,
                        row,
                        "duplicate value",
                        field="wikidata",
                        value=row["wikidata"],
                    )
                )
            elif wikidata:
                wikidatas[row["wikidata"]] = organisation

            # check LPA value against dataset
            lpa = row.get("local-planning-authority", "")
            if not lpa:
                if (
                    row["dataset"] in ["local-authority", "national-park-authority"]
                ) and (
                    row.get("local-authority-type", "") not in ["CTY", "COMB", "SRA"]
                ):
                    severity = "warning" if row.get("end-date", "") else "error"
                    issues.append(
                        issue(
                            severity, row, "missing", field="local-planning-authority"
                        )
                    )
            elif lpa not in lpas:
                issues.append(
                    issue(
                        "error",
                        row,
                        "unknown",
                        field="local-planning-authority",
                        value=lpa,
                    )
                )
            else:
                lpas[lpa]["organisation"] = organisation

            # check billing-authority
            ba = row.get("billing-authority", "")
            if not ba:
                if row["dataset"] not in ["government-organisation"]:
                    severity = "warning" if row.get("end-date", "") else "error"
                    issues.append(
                        issue(severity, row, "missing", field="billing-authority")
                    )
            elif ba in bas:
                issues.append(
                    issue(
                        "error",
                        row,
                        "duplicate value",
                        field="billing-authority",
                        value=row["billing-authority"],
                    )
                )
            else:
                bas[row["billing-authority"]] = organisation

            # check opendatacommunities-uri
            odc = row.get("opendatacommunities-uri", "")
            if not odc:
                if row["dataset"] not in ["government-organisation"]:
                    severity = "warning" if row.get("end-date", "") else "error"
                    issues.append(
                        issue(severity, row, "missing", field="opendatacommunities-uri")
                    )
            elif odc in odcs:
                issues.append(
                    issue(
                        "error",
                        row,
                        "duplicate value",
                        field="opendatacommunities-uri",
                        value=row["opendatacommunities-uri"],
                    )
                )
            else:
                odcs[row["opendatacommunities-uri"]] = organisation

        save_issues(issues, output_path)

    def fetch_dataset(self):
        os.makedirs(self.cache_dir, exist_ok=True)

        for csv_name in self.source_filenames():
            r = requests.get(self.download_url + csv_name)
            if r.status_code == 200:
                with open(os.path.join(self.cache_dir, csv_name), "wb") as t:
                    t.write(r.content)
