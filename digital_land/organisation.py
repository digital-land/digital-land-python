import re
import csv


uri_basename_re = re.compile(r".*/")


def uri_basename(value):
    return uri_basename_re.sub("", value.rstrip("/").lower())


def lower_uri(value):
    return "".join(value.split()).lower()


class Organisation:
    organisation_path = "var/cache/organisation.csv"
    organisation = {}
    organisation_uri = {}

    def __init__(self, organisation_path=None):
        if organisation_path:
            self.organisation_path = organisation_path
        self.load_organisation()

    def load_organisation(self):
        for row in csv.DictReader(open(self.organisation_path)):
            self.organisation[row["organisation"]] = row
            if "opendatacommunities" in row:
                uri = row["opendatacommunities"].lower()
                self.organisation_uri[row["organisation"].lower()] = uri
                self.organisation_uri[uri] = uri
                self.organisation_uri[uri_basename(uri)] = uri
                self.organisation_uri[row["statistical-geography"].lower()] = uri
                if "local-authority-eng" in row["organisation"]:
                    dl_url = "https://digital-land.github.io/organisation/%s/" % (
                        row["organisation"]
                    )
                    dl_url = dl_url.lower().replace("-eng:", "-eng/")
                    self.organisation_uri[dl_url] = uri

        # TODO get the patch details from Pipeline
        for row in csv.DictReader(open("pipeline/patch.csv")):
            if row["field"] == "OrganisationURI":
                self.organisation_uri[lower_uri(row["pattern"])] = row["value"]
