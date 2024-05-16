import csv
import logging
from os import listdir
from pathlib import Path

from .csv import CsvPackage

logger = logging.getLogger(__name__)


class OrganisationPackage(CsvPackage):
    def __init__(self, flattened_dir, **kwargs):
        self.flattened_dir = flattened_dir
        super().__init__("organisation", tables={"organisation": None}, **kwargs)

    def create(self):
        # get field names
        org_field_names = self.specification.schema_field["organisation"]

        # get get file list
        filenames = listdir(self.flattened_dir)
        filenames = [filename for filename in filenames if filename.endswith(".csv")]

        orgs = []
        for file in filenames:
            filepath = Path(self.flattened_dir) / file
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
