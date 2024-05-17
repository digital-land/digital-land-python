import pytest
import csv
import os
import urllib.request

from digital_land.package.organisation import OrganisationPackage


@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    source_url = "https://raw.githubusercontent.com/digital-land/"
    specification_csvs = [
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
        # deprecated ..
        "pipeline.csv",
        "dataset-schema.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for specification_csv in specification_csvs:
        urllib.request.urlretrieve(
            f"{source_url}/specification/main/specification/{specification_csv}",
            os.path.join(specification_dir, specification_csv),
        )

    return specification_dir


flattened_data = {
    "government-organisation.csv": (
        "dataset,end-date,entity,entry-date,geojson,geometry,name,organisation-entity,point,prefix,reference,"
        "start-date,typology,addressbase-custodian,billing-authority,company,notes,opendatacommunities-uri,"
        "parliament-thesaurus,statistical-geography,twitter,website,website-url,wikidata,wikipedia",
        [
            {
                "dataset": "government-organisation",
                "entity": "600001",
                "entry-date": "2023-10-06",
                "name": "Department for Levelling Up, Housing and Communities",
                "organisation-entity": "600001",
                "prefix": "government-organisation",
                "reference": "D1342",
                "start-date": "2021-09-20",
                "typology": "organisation",
                "twitter": "luhc",
                "website": "https://www.gov.uk/government/organisations/department-for-levelling-up-housing-and-communities",
                "wikidata": "Q601819",
                "wikipedia": "Department_for_Levelling_Up",
            }
        ],
    )
}


@pytest.fixture(scope="session")
def flattened_dir(tmp_path_factory):
    flattened_dir = tmp_path_factory.mktemp("flattened")

    for filename, (fieldnames, rows) in flattened_data.items():
        with open(os.path.join(flattened_dir, filename), "w") as f:
            writer = csv.DictWriter(f, fieldnames.split(","))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    return flattened_dir


# Check a row contains the data we expect, and only the data we expect
def check_row(row, expected):
    for key, value in expected.items():
        assert key in row
        assert row[key] == value
    for key, value in row.items():
        if key not in expected:
            assert row[key] == ""


def test_organisation_package(
    tmp_path,
    specification_dir,
    flattened_dir,
):
    package = OrganisationPackage(
        specification_dir=specification_dir,
        flattened_dir=flattened_dir,
        path=os.path.join(tmp_path, "organisation.csv"),
    )

    # create package
    package.create()

    # Check the output
    with open(package.path, "r", encoding="UTF8", newline="") as f:
        rows = [row for row in csv.DictReader(f)]

        assert len(rows) == 1
        check_row(
            rows[0],
            {
                "addressbase-custodian": "",
                "billing-authority": "",
                "census-area": "",
                "combined-authority": "",
                "company": "",
                "dataset": "government-organisation",
                "end-date": "",
                "entity": "600001",
                "entry-date": "2023-10-06",
                "esd-inventory": "",
                "local-authority-type": "",
                "local-resilience-forum": "",
                "local-planning-authority": "",
                "local-authority-district": "",
                "name": "Department for Levelling Up, Housing and Communities",
                "national-park": "",
                "notes": "",
                "official-name": "",
                "opendatacommunities-uri": "",
                "organisation": "government-organisation:D1342",
                "parliament-thesaurus": "",
                "prefix": "government-organisation",
                "reference": "D1342",
                "region": "",
                "shielding-hub": "",
                "start-date": "2021-09-20",
                "statistical-geography": "",
                "website": "https://www.gov.uk/government/organisations/department-for-levelling-up-housing-and-communities",
                "wikidata": "Q601819",
                "wikipedia": "Department_for_Levelling_Up",
            },
        )
