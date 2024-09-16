import csv
import os
import requests
import logging

DEFAULT_URL = "https://files.planning.data.gov.uk"


class API:
    def __init__(self, url=DEFAULT_URL, cache_dir="var/cache"):
        self.url = url
        self.cache_dir = cache_dir

    def download_dataset(self, dataset, overwrite=False, csv_path=None):
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

    def get_valid_category_values(self, category_fields):
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
            if os.stat(csv_path).st_size == 0:
                valid_category_values[category_field] = []
            else:
                with open(csv_path, mode="r") as file:
                    valid_category_values[category_field] = [
                        row["reference"].lower()
                        for row in csv.DictReader(file)
                        if row.get("reference")
                    ]

        return valid_category_values
