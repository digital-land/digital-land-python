import pytest
import csv
from tests.utils.helpers import hash_digest, execute

ENDPOINT = "https://raw.githubusercontent.com/digital-land/digital-land-python/main/tests/data/resource_examples/csv.csv"
COLLECTION_DIR = "./collection"


@pytest.fixture()
def create_collection(tmp_path):
    collection_dir = tmp_path / "collection"



def _create_endpoint_csv(endpoint_url, collection_dir):
    e = collection_dir / "endpoint.csv"
    fieldnames = ["endpoint", "endpoint-url"]
    with open(e, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"endpoint": hash_digest(ENDPOINT), "endpoint-url": ENDPOINT})

def _create_source_csv(endpoint_url, collection_dir):
    s = collection_dir / "source.csv"
    fieldnames = ["endpoint", "pipelines", "organisation"]
    with open(s, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"endpoint": hash_digest(ENDPOINT), "pipelines": ENDPOINT})
