import csv
import json
import logging
from os import listdir
from pathlib import Path

from .csv import CsvPackage

logger = logging.getLogger(__name__)


class OrganisationPackage(CsvPackage):
    def __init__(self, **kwargs):
        self.flattened_dir = kwargs.pop("flattened_dir", None)
        self.dataset_dir = kwargs.pop("dataset_dir", None)
        super().__init__("organisation", tables={"organisation": None}, **kwargs)

    def create(self):
        if self.dataset_dir:
            return self.create_from_dataset()

        if self.flattened_dir:
            return self.create_from_flattened()

        raise RuntimeError("Neither flattened nor dataset directory specified.")

    def create_from_flattened(self):
        # get field names
        org_field_names = self.specification.schema["organisation"]["fields"]

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

    def create_from_dataset(self):
        # get field names
        org_field_names = self.specification.schema["organisation"]["fields"]

        # get get file list
        filenames = listdir(self.dataset_dir)
        filenames = [filename for filename in filenames if filename.endswith(".csv")]

        orgs = []
        for file in filenames:
            filepath = Path(self.dataset_dir) / file
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
