"""SDK for interacting with the Digital Land platform. Built primarily using polars as the focus is on a single machine"""

import requests
import polars as pl

from urllib.parse import urljoin


class Platform:
    """
    The Platform class provides methods to interact with the Digital Land platform.
    It is designed to work with datasets, collections, and tables within the platform.
    """

    def __init__(
        self,
    ):
        self.api_url = "https://planning.data.gov.uk"
        #  files url to be used for downloading bulk data files
        self.files_url = "https://files.planning.data.gov.uk"
        # Initialize other attributes as needed
        # e.g., self.dataset_path_name, self.tables, etc.
        # self.user_agent= useer_agent
        # self.session = None

    def get_session(self, session):
        if self.session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": self.user_agent})
            self.session = session
        else:
            session = self.session
        return session

    def _get_dataset_file_url(self, dataset: str, extension="csv"):
        """
        small function to generate the dataset url mainly create to mock the path for testing
        """

        dataset_url = urljoin(self.files_url, "dataset", f"{dataset}.{extension}")
        return dataset_url

    def download_dataset(self, output_path: str, dataset: str, extension="csv"):
        """
        Downloads a dataset from the platform.

        Args:
            dataset (str): The path to the dataset to be downloaded.
        """
        print(output_path)
        # download dataset from url
        dataset_url = self._get_dataset_file_url(dataset, extension)

        df = pl.read_csv(
            dataset_url,
            infer_schema_length=1000,
            try_parse_dates=False,
            low_memory=True,
        )

        # could comeback and add filters
        # filtered = df.filter(pl.col("dataset") == dataset)
        # filtered.write_csv(output_path)
        print(output_path)
        df.write_csv(output_path)
