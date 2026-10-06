import pytest

from digital_land.package.organisation import organisation_datasets

ROWS = [
    {
        "dataset": "in-production",
        "typology": "organisation",
        "collection": "organisation",
        "environment": "production",
        "end-date": "",
    },
    {
        "dataset": "in-staging",
        "typology": "organisation",
        "collection": "organisation",
        "environment": "staging",
        "end-date": "",
    },
    {
        "dataset": "in-development",
        "typology": "organisation",
        "collection": "organisation",
        "environment": "development",
        "end-date": "",
    },
    {
        "dataset": "switched-off",
        "typology": "organisation",
        "collection": "organisation",
        "environment": "",
        "end-date": "",
    },
    {
        "dataset": "end-dated",
        "typology": "organisation",
        "collection": "organisation",
        "environment": "production",
        "end-date": "2026-01-01",
    },
    {
        "dataset": "no-collection",
        "typology": "organisation",
        "collection": "",
        "environment": "production",
        "end-date": "",
    },
    {
        "dataset": "not-an-organisation",
        "typology": "geography",
        "collection": "organisation",
        "environment": "production",
        "end-date": "",
    },
]


@pytest.mark.parametrize(
    "environment, expected",
    [
        ("production", ["in-production"]),
        ("staging", ["in-production", "in-staging"]),
        ("development", ["in-production", "in-staging", "in-development"]),
    ],
)
def test_organisation_datasets_for_each_environment(environment, expected):
    assert organisation_datasets(ROWS, environment) == expected


@pytest.mark.parametrize("environment", [None, ""])
def test_organisation_datasets_without_environment_ignores_environment(environment):
    """Callers that don't set an environment get every organisation dataset with
    a collection, which matches the hard-coded list this replaced"""
    assert organisation_datasets(ROWS, environment) == [
        "in-production",
        "in-staging",
        "in-development",
        "switched-off",
    ]
