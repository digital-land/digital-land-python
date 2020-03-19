import csv
import re
from ..fetch import fetch
from ..dataset import datasets
from .enum import EnumDataType


uri_basename_re = re.compile(r".*/")


def uri_basename(value):
    return uri_basename_re.sub("", value.rstrip("/").lower())


class OrganisationURIDataType(EnumDataType):
    def __init__(
        self, name="OrganisationURI", dataset="organisation", patches_path=None
    ):
        super().__init__(name=name, dataset=dataset, patches_path=patches_path)

    def load_dataset(self, dataset):
        for row in csv.DictReader(
            open(fetch(datasets[dataset]["resource-url"]), newline="")
        ):
            if "opendatacommunities" in row:
                uri = row["opendatacommunities"]

                self.add_enum(uri)

                # some publishers just use the end of the URI
                self.add_value(uri, uri_basename(uri))

                # some publishers use the ONS/GSS area code
                self.add_value(uri, row["statistical-geography"])

                # some publishers have used the digital-land URL
                if "local-authority-eng" in row["organisation"]:
                    dl_url = "https://digital-land.github.io/organisation/%s/" % (
                        row["organisation"]
                    )
                    self.add_value(uri, dl_url.replace("-eng:", "-eng/"))
