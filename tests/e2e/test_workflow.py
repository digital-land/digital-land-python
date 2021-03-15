import pytest
import csv
import filecmp
from pathlib import Path

from tests.utils.helpers import hash_digest, execute, print_diffs

ENDPOINT = "https://raw.githubusercontent.com/digital-land/digital-land-python/e2e_test/tests/data/resource_examples/e2e.csv"


@pytest.fixture()
def create_workspace(tmp_path):
    (tmp_path / "issue").mkdir()
    (tmp_path / "output").mkdir()
    c = tmp_path / "collection"
    c.mkdir()
    _create_endpoint_csv(ENDPOINT, c)
    _create_source_csv(ENDPOINT, c)
    return tmp_path


def _create_endpoint_csv(endpoint_url, collection_dir):
    e = collection_dir / "endpoint.csv"
    fieldnames = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    with open(e, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": hash_digest(endpoint_url),
                "endpoint-url": endpoint_url,
                "entry-date": "01/02/2003",
                "start-date": "01/02/2003",
            }
        )


def _create_source_csv(endpoint_url, collection_dir):
    s = collection_dir / "source.csv"
    fieldnames = [
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]
    with open(s, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": hash_digest(endpoint_url),
                "pipelines": "schema-three",
                "entry-date": "01/02/2003",
                "start-date": "01/02/2003",
            }
        )


def test_workflow(create_workspace):
    tmp_path = create_workspace
    returncode, outs, errs = execute(
        [
            "digital-land",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "collect",
            "-c",
            str(tmp_path / "collection"),
            str(tmp_path / "collection" / "endpoint.csv"),
        ]
    )

    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs

    collected_resources = [
        x for x in Path(tmp_path / "collection" / "resource").glob("*")
    ]
    assert len(collected_resources) == 1
    resource = collected_resources[0]

    returncode, outs, errs = execute(
        [
            "digital-land",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "collection-save-csv",
            "-c",
            str(tmp_path / "collection"),
        ]
    )

    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs
    assert (tmp_path / "collection" / "log.csv").is_file()
    assert (tmp_path / "collection" / "resource.csv").is_file()

    returncode, outs, errs = execute(
        [
            "digital-land",
            "-p",
            "tests/data/pipeline",
            "-s",
            "tests/data/specification",
            "--pipeline-name",
            "pipeline-three",
            "pipeline",
            "-c",
            str(tmp_path / "collection"),
            "-o",
            "tests/data/organisation.csv",
            "-i",
            str(tmp_path / "issue"),
            str(resource),
            str(tmp_path / "output" / "result.csv"),
        ]
    )

    result = tmp_path / "output" / "result.csv"

    assert returncode == 0, f"return code non-zero: {errs}"
    assert "ERROR" not in errs
    assert result.is_file()
    assert filecmp.cmp(result, "tests/data/expected_output/e2e.csv"), print_diffs(
        result, "tests/data/expected_output/e2e.csv"
    )
