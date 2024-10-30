import csv
import os
import pytest


@pytest.fixture
def issue_dir(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")

    issue_paths = []
    issue_paths.append(os.path.join(issue_dir, "dataset1/resource1.csv"))
    issue_paths.append(os.path.join(issue_dir, "dataset2/resource2.csv"))

    field_names = ["header1", "header2", "header3"]
    rows = [{"header1": "value1", "header2": "value2", "header3": "value3"}]
    for issue_path in issue_paths:
        os.makedirs(os.path.dirname(issue_path), exist_ok=True)
        with open(issue_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(rows)

    return issue_dir
