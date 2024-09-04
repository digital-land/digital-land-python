import re
import csv
import logging
from pathlib import Path


uri_basename_re = re.compile(r".*/")


def uri_basename(value):
    return uri_basename_re.sub("", value.rstrip("/").lower())


def lower_uri(value):
    return "".join(value.split()).lower()


class Organisation:
    organisation_path = "var/cache/organisation.csv"
    pipeline_patch_path = "pipeline/patch.csv"
    organisation = {}
    organisation_uri = {}
    organisation_lookup = {}

    def __init__(self, organisation_path, pipeline_dir, organisation=None):
        if organisation_path:
            self.organisation_path = organisation_path
        if pipeline_dir:
            self.pipeline_patch_path = pipeline_dir / "patch.csv"
        if organisation is None:
            self.load_organisation()
        else:
            self.organisation = organisation

    def load_organisation(self):
        for row in csv.DictReader(open(self.organisation_path)):
            self.organisation[row["organisation"]] = row
            self.organisation_lookup[row["organisation"]] = row["organisation"]

            if "opendatacommunities-uri" in row:
                uri = row["opendatacommunities-uri"].lower()
                self.organisation_lookup[uri] = row["organisation"]
                self.organisation_uri[row["organisation"].lower()] = uri
                self.organisation_uri[uri] = uri
                self.organisation_uri[uri.replace("http:", "https:")] = uri
                self.organisation_uri[uri.replace("/id/", "/doc/")] = uri
                self.organisation_uri[
                    uri.replace("/id/", "/doc/").replace("http:", "https:")
                ] = uri
                self.organisation_uri[uri_basename(uri)] = uri
                self.organisation_uri[row["statistical-geography"].lower()] = uri
                if "local-authority-eng" in row["organisation"]:
                    dl_url = "https://digital-land.github.io/organisation/%s/" % (
                        row["organisation"]
                    )
                    dl_url = dl_url.lower().replace("-eng:", "-eng/")
                    self.organisation_uri[dl_url] = uri

        for key, uri in self.organisation_uri.items():
            self.organisation_lookup[key] = self.organisation_lookup[
                self.organisation_uri[uri]
            ]

        # TODO get the patch details from Pipeline
        if Path(self.pipeline_patch_path).exists():
            for row in csv.DictReader(open(self.pipeline_patch_path)):
                if row["field"] == "OrganisationURI":
                    self.organisation_uri[lower_uri(row["pattern"])] = row["value"]

        for key in self.organisation:
            self.organisation_lookup[key.lower()] = key

    def lookup(self, organisation):
        if not organisation:
            return organisation

        for leg in [
            "http://opendatacommunities.org/resource?uri=http://opendatacommunities.org/id/geography/administration/nmd/",
            "http://opendatacommunities.org/id/geography/administration/nmd/",
            "http://opendatacommunities.org/id/geography/administration/ua/",
            "http://opendatacommunities.org/id/geography/administration/npark/",
        ]:
            if organisation.startswith(leg):
                organisation = organisation[len(leg) :]

        if organisation.lower() not in self.organisation_lookup:
            if organisation.lower().replace("-eng", "") in self.organisation_lookup:
                return self.organisation_lookup[
                    organisation.lower().replace("-eng", "")
                ]
            else:
                logging.info(f"unknown organisation {organisation}")
                return ""
        return self.organisation_lookup[organisation.lower()]
