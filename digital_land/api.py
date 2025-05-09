import csv
import os
import requests
import logging
import typing

from digital_land.pipeline.main import Pipeline

DEFAULT_URL = "https://files.planning.data.gov.uk"


class API:
    def __init__(
        self, specification, url: str = DEFAULT_URL, cache_dir: str = "var/cache"
    ):
        """Create the API object.
        url: CDN url to get files from (defaults to production CDN)
        cache_dir: directory to use for caching downloaded content.
        """
        self.specification = specification
        self.url = url
        self.cache_dir = cache_dir

    def download_dataset(
        self,
        dataset: str,
        overwrite: bool = False,
        path: str = None,
        sqlite: bool = False,
    ):
        """Downloads a dataset as a .csv.
        dataset: dataaset name
        overwrite: overwrite file is it already exists (otherwise will just return)
        path: file to download to (otherwise <cache-dir>/dataset/<dataset-name>.csv/.sqlite3)
        Returns: None.
        sqlite: if True dataset will be downloaded as .sqlite3 instead of .csv
        The file fill be downloaded to the given path or cache, unless an exception occurs.
        """
        extension = ".sqlite3" if sqlite else ".csv"
        if path is None:
            path = os.path.join(self.cache_dir, "dataset", f"{dataset}{extension}")

        if os.path.exists(path) and not overwrite:
            logging.info(f"Dataset file {path} already exists.")
            return
        # we need to collection to find the .sqlite3 url
        url = f"{self.url}/dataset/{dataset}{extension}"
        # url = "https://files.planning.data.gov.uk/tree-preservation-order-collection/dataset/tree-preservation-order.sqlite3"
        url = "https://files.planning.data.gov.uk/central-activities-zone-collection/dataset/central-activities-zone.sqlite3"

        response = requests.get(url)
        response.raise_for_status()

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(response.text)

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
