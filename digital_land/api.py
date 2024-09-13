import csv
import os
import requests
import logging


class API:
    def __init__(self, url, cache_dir=None):
        self.url = url
        self.cache_dir = cache_dir

    def download_dataset(self, dataset):
        entities = []
        count = 0
        url = f"{self.url}/entity.json?dataset={dataset}&limit=500"

        logging.info(f"Downloading dataset {dataset}")

        while url:
            logging.info(f"Downloading from {url}")

            response = requests.get(url)
            response.raise_for_status()

            entities += response.json()["entities"]

            url = response.json()["links"].get("next", "")
            count = response.json()["count"]

        if len(entities) != count:
            print(f"Downloaded {len(entities)} but expected {count}")

        dataset_dir = os.path.join(self.cache_dir, "dataset")
        csv_file = os.path.join(dataset_dir, dataset + ".csv")
        os.makedirs(dataset_dir, exist_ok=True)

        with open(csv_file, "w") as f:
            if len(entities) > 0:
                writer = csv.DictWriter(f, fieldnames=entities[0].keys())
                writer.writeheader()
                writer.writerows(entities)

        logging.info(f"Saved to {csv_file}")

    def get_valid_category_values(self, category_fields):
        valid_category_values = {}

        for category_field in category_fields:
            csv_file = os.path.join(self.cache_dir, "dataset", f"{category_field}.csv")

            # If we don't have the file cached, try to download it
            if not os.path.exists(csv_file):
                try:
                    self.download_dataset(category_field)
                except Exception as ex:
                    logging.warning(
                        f"Unable to download category values for '{category_field}' ({ex}). These will not be checked."
                    )
                    # Write an empty file do we don't try again
                    with open(csv_file, mode="w") as file:
                        pass

            # If the file is empty just skip this one
            if os.stat(csv_file).st_size == 0:
                continue

            with open(csv_file, mode="r") as file:
                valid_category_values[category_field] = [
                    row["reference"].lower()
                    for row in csv.DictReader(file)
                    if row.get("reference")
                ]

        return valid_category_values
