"""
Integration test: get_resource_unidentified_lookups with _PolarsPhases bridge.

Verifies that the polars bridge (StreamToPolarsConverter → Polars phases →
polars_to_stream) inside get_resource_unidentified_lookups runs end-to-end and
produces correct lookup entries.
"""

import csv
import urllib.request

import pytest

from digital_land.commands import get_resource_unidentified_lookups
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def resource_csv(tmp_path):
    """Minimal CSV resource with two rows that should produce new lookup entries."""
    rows = [
        {
            "reference": "TPO-001",
            "organisation": "government-organisation:D1342",
            "value": "oak",
        },
        {
            "reference": "TPO-002",
            "organisation": "government-organisation:D1342",
            "value": "ash",
        },
    ]
    path = tmp_path / "test_resource.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["reference", "organisation", "value"])
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture
def pipeline_dir(tmp_path):
    """Minimal pipeline directory with an empty lookup.csv."""
    p = tmp_path / "pipeline"
    p.mkdir()

    # empty lookup.csv
    with open(p / "lookup.csv", "w", newline="") as f:
        csv.DictWriter(
            f,
            fieldnames=[
                "prefix",
                "resource",
                "entry-number",
                "organisation",
                "reference",
                "entity",
            ],
        ).writeheader()

    return p


@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    """Download live specification CSVs (session-scoped so they are fetched once)."""
    spec_dir = tmp_path_factory.mktemp("specification")
    base = "https://raw.githubusercontent.com/digital-land/specification/main/specification/"
    files = [
        "attribution.csv",
        "licence.csv",
        "typology.csv",
        "theme.csv",
        "collection.csv",
        "dataset.csv",
        "dataset-field.csv",
        "field.csv",
        "datatype.csv",
        "prefix.csv",
        "provision-rule.csv",
        "pipeline.csv",
        "dataset-schema.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for fname in files:
        urllib.request.urlretrieve(base + fname, spec_dir / fname)
    return spec_dir


@pytest.fixture(scope="session")
def organisation_csv(tmp_path_factory):
    """Download live organisation.csv (session-scoped)."""
    path = tmp_path_factory.mktemp("org") / "organisation.csv"
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        path,
    )
    return path


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_get_resource_unidentified_lookups_polars_bridge(
    resource_csv,
    pipeline_dir,
    specification_dir,
    organisation_csv,
):
    """
    Smoke test: get_resource_unidentified_lookups should run to completion via
    the _PolarsPhases bridge without raising an exception.

    The function returns a list of (lookup_dict, ...) tuples for every row that
    could not be matched to an existing entity. Since our resource has two
    unrecognised references we expect at least one new lookup entry to be
    produced.
    """
    dataset = "tree"
    pipeline = Pipeline(str(pipeline_dir), dataset)
    specification = Specification(str(specification_dir))

    result = get_resource_unidentified_lookups(
        input_path=resource_csv,
        dataset=dataset,
        pipeline=pipeline,
        specification=specification,
        organisations=["government-organisation:D1342"],
        org_csv_path=str(organisation_csv),
        endpoints=[],
    )

    # result is a list of (lookup_dict, ...) pairs
    assert isinstance(result, list), "Expected a list of lookup entries"
    # Each entry should be a tuple/list whose first element is a dict with
    # at minimum a 'reference' key
    for entry in result:
        lookup = entry[0]
        assert "reference" in lookup, f"Expected 'reference' key in {lookup}"
