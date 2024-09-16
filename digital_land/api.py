import csv
import os
import requests
import logging
import typing

DEFAULT_URL = "https://files.planning.data.gov.uk"


class API:
    def __init__(self, url: str = DEFAULT_URL, cache_dir: str = "var/cache"):
        """Create the API object.
        url: CDN url to get files from (defaults to production CDN)
        cache_dir: directory to use for caching downloaded content.
        """
        self.url = url
        self.cache_dir = cache_dir

    def download_dataset(
        self, dataset: str, overwrite: bool = False, csv_path: str = None
    ):
        """Downloads a dataset.
        dataset: dataaset name
        overwrite: overwrite file is it already exists (otherwise will just return)
        csv_path: file to download to (otherwise <cache-dir>/dataset/<dataset-name>.csv)
        Returns: None. The file fill be downloaded to the given path or cache, unless an exception occurs.
        """
        if csv_path is None:
            csv_path = os.path.join(self.cache_dir, "dataset", f"{dataset}.csv")

        if os.path.exists(csv_path) and not overwrite:
            logging.info(f"Dataset file {csv_path} already exists.")
            return

        url = f"{self.url}/dataset/{dataset}.csv"

        response = requests.get(url)
        response.raise_for_status()

        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, "w") as f:
            f.write(response.text)

        logging.info(f"Downloaded dataset {dataset} from {url} to {csv_path}")

    def get_valid_category_values(
        self, category_fields: typing.Iterable[str]
    ) -> typing.Mapping[str, typing.Iterable[str]]:
        """gets the valid caregory values.
        category_fields: Iterable category fields to get valid values for
        Returns: Mapping of field to valid values. If the valid values cannot be obtained, it will be omitted.
        """
        valid_category_values = {}

        for category_field in category_fields:
            csv_path = os.path.join(self.cache_dir, "dataset", f"{category_field}.csv")

            # If we don't have the file cached, try to download it
            if not os.path.exists(csv_path):
                try:
                    self.download_dataset(
                        category_field, overwrite=False, csv_path=csv_path
                    )
                except Exception as ex:
                    logging.warning(
                        f"Unable to download category values for '{category_field}' ({ex}). These will not be checked."
                    )
                    # Write an empty file do we don't try again
                    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                    with open(csv_path, mode="w") as file:
                        pass

            # Don't bother trying to load empty files
            if os.stat(csv_path).st_size > 0:
                with open(csv_path, mode="r") as file:
                    valid_category_values[category_field] = [
                        row["reference"].lower()
                        for row in csv.DictReader(file)
                        if row.get("reference")
                    ]

        return valid_category_values
