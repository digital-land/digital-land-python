import re
from .datatype import DataType

organisation_uri = None
default_values = None


end_of_uri_re = re.compile(r".*/")


def end_of_uri(value):
    return end_of_uri_re.sub("", value.rstrip("/").lower())


def lower_uri(value):
    return "".join(value.split()).lower()


# OrganisationURI values
def load_organisations(path="var/cache/organisation.csv"):
    organisation_uri = {}
    organisation = {}
    for row in csv.DictReader(open(organisations), newline=""):
        organisation[row["organisation"]] = row
        if "opendatacommunities" in row:

            uri = row["opendatacommunities"].lower()

            organisation_uri[row["organisation"].lower()] = uri
            organisation_uri[uri] = uri
            organisation_uri[end_of_uri(uri)] = uri
            organisation_uri[row["statistical-geography"].lower()] = uri

            if "local-authority-eng" in row["organisation"]:
                dl_url = "https://digital-land.github.io/organisation/%s/" % (
                    row["organisation"]
                )
                dl_url = dl_url.lower().replace("-eng:", "-eng/")
                organisation_uri[dl_url] = uri

    return organisation_uri


# OrganisationURI patches
def load_organisation_patches(path="patch/organisation.csv"):
    for row in csv.DictReader(open(path, newline="")):
        value = lower_uri(row["value"])
        if row["organisation"]:
            organisation_uri[value] = organisation[row["organisation"]][
                "opendatacommunities"
            ]


# deduce default OrganisationURI and LastUpdatedDate from path
def load_resource_defaults(input_path, path="index/resource-organisation.csv"):
    default_values = {}
    organisation = ""
    for row in csv.DictReader(open(path), newline=""):
        if row["resource"] in input_path:
            default_values["LastUpdatedDate"] = row["start-date"]
            if not organisation:
                organisation = row["organisation"]
            elif organisation != row["organisation"]:
                # resource has more than one organisation
                default_values["OrganisationURI"] = ""
                return
    default_values["OrganisationURI"] = organisation_uri[organisation.lower()]
    return default_values


class OrganisationURIDataType(DataType):
    def __init__(self, input_path=None):
        if not organisation_uri:
            organisation_uri = load_organisations()
            load_organisation_patches()

        if input_path and not default_values:
            default_values = load_resource_defaults(input_path)

    def normalise(self, fieldvalue, issues=None):
        value = lower_uri(fieldvalue)

        if value in organisation_uri:
            return organisation_uri[value]

        s = end_of_uri(value)
        if s in organisation_uri:
            return organisation_uri[s]

        issues.log("opendatacommunities-uri", fieldvalue)
        return ""
