from datetime import datetime
import os

from digital_land.utils.add_data_utils import clear_log


def test_clear_logs(tmp_path_factory):
    today = datetime.utcnow().isoformat()[:10]
    endpoint = "endpoint"
    collection_dir = tmp_path_factory.mktemp("random_collection")

    file_path = os.path.join(collection_dir, "log", today, f"{endpoint}.json")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write("hello")

    clear_log(collection_dir, endpoint)

    assert not os.path.isfile(file_path)
