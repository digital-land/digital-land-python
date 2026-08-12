import csv
from enum import Enum
import os
import requests
import logging
import tempfile
import typing

from digital_land.pipeline.main import Pipeline
from digital_land.specification import Specification

DEFAULT_URL = "https://files.planning.data.gov.uk"


class API:
    def __init__(
        self,
        specification: Specification,
        url: str = DEFAULT_URL,
        cache_dir: str = "var/cache",
    ):
        """Create the API object.
        url: CDN url to get files from (defaults to production CDN)
        cache_dir: directory to use for caching downloaded content.
        """
        self.specification = specification
        self.url = url
        self.cache_dir = cache_dir

    class Extension(str, Enum):
        CSV = "csv"
        SQLITE3 = "sqlite3"

    def download_dataset(
        self,
        dataset: str,
        overwrite: bool = False,
        path: str = None,
        extension: Extension = Extension.CSV,
    ):
        """
        Downloads a dataset in CSV or SQLite3 format.
        - dataset: dataset name.
        - overwrite: overwrite file is it already exists (otherwise will just return).
        - path: file to download to (otherwise <cache-dir>/dataset/<dataset-name>.<extension>).
        - extension: 'csv' or 'sqlite3', 'csv' by default.
        - Returns: None.
        The file will be downloaded to the given path or cache, unless an exception occurs.

        """
        if path is None:
            path = os.path.join(self.cache_dir, "dataset", f"{dataset}.{extension}")

        if os.path.exists(path) and not overwrite:
            logging.info(f"Dataset file {path} already exists.")
            return

        # different extensions require different urls
        if extension == self.Extension.SQLITE3:
            collection = self.specification.dataset[dataset]["collection"]
            url = f"{self.url}/{collection}-collection/dataset/{dataset}.sqlite3"
        else:
            url = f"{self.url}/dataset/{dataset}.csv"

        response = requests.get(url)
        response.raise_for_status()

        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)

        # Write to a temporary file in the same directory, then rename it into
        # place. os.replace is atomic, so other processes only ever see a
        # complete file - never one that is empty or half-written.
        fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(response.content)
            os.replace(tmp_path, path)
        except BaseException:
            os.unlink(tmp_path)
            raise

        logging.info(f"Downloaded dataset {dataset} from {url} to {path}")

    def get_valid_category_values(
        self, dataset: str, pipeline: Pipeline
    ) -> typing.Mapping[str, typing.Iterable[str]]:
        """gets the valid caregory values.
        category_fields: Iterable category fields to get valid values for
        Returns: Mapping of field to valid values.
        If the valid values cannot be obtained, that field will be omitted.
        """
        valid_category_values = {}

        for category_field in self.specification.get_category_fields(dataset=dataset):
            field_dataset = (
                self.specification.dataset_field_dataset[dataset][category_field]
                or category_field
            )

            csv_path = os.path.join(self.cache_dir, "dataset", f"{field_dataset}.csv")

            # If we don't have the file cached, try to download it
            if not os.path.exists(csv_path):
                try:
                    self.download_dataset(field_dataset, overwrite=False, path=csv_path)
                except Exception as ex:
                    logging.warning(
                        f"Unable to download category values '{field_dataset}' ({ex}). These will not be checked."
                    )
                    # Write an empty file do we don't try again
                    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                    with open(csv_path, mode="w") as file:
                        pass

            # Don't bother trying to load empty files
            if os.stat(csv_path).st_size > 0:
                with open(csv_path, mode="r") as file:
                    values = [
                        row["reference"]
                        for row in csv.DictReader(file)
                        if row.get("reference")
                    ]
                    valid_category_values[category_field] = values

                    # Check for replacement field
                    replacement_field = pipeline.migrate.get(category_field, None)
                    if replacement_field:
                        valid_category_values[replacement_field] = values

        return valid_category_values
