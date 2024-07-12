import pytest
import json
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


dataset_data = {
    # This should be in the output file - it's one of the included files, and has the right typology
    "government-organisation.csv": (
        "dataset,end_date,entity,entry_date,geojson,geometry,json,name,organisation_entity,point,prefix,reference,start_date,typology",
        [
            {
                "dataset": "government-organisation",
                "entity": "600001",
                "entry_date": "2023-10-06",
                "json": json.dumps(
                    {
                        "twitter": "luhc",
                        "website": "https://www.gov.uk/government/organisations/department-for-levelling-up-housing-and-communities",
                        "wikidata": "Q601819",
                        "wikipedia": "Department_for_Levelling_Up",
                    }
                ),
                "name": "Department for Levelling Up, Housing and Communities",
                "organisation_entity": "600001",
                "prefix": "government-organisation",
                "reference": "D1342",
                "start_date": "2021-09-20",
                "typology": "organisation",
            }
        ],
    ),
    # This one shouldn't be in the output file - it's got the right typology, but isn't one of the included files.
    "null-organisation.csv": (
        "dataset,end_date,entity,entry_date,geojson,geometry,json,name,organisation_entity,point,prefix,reference,start_date,typology",
        [
            {
                "dataset": "null-organisation",
                "entity": "600001",
                "entry_date": "2023-10-06",
                "json": json.dumps(
                    {
                        "twitter": "x",
                        "website": "http://localhost",
                        "wikidata": "Q601819",
                        "wikipedia": "Nowhere",
                    }
                ),
                "name": "The Null Organisation",
                "organisation_entity": "900001",
                "prefix": "null-organisation",
                "reference": "D9876",
                "start_date": "2021-09-20",
                "typology": "organisation",
            }
        ],
    ),
    # This one shouldn't be in the output file - it's one of the included files, but it's got the right typology.
    "local-authority.csv": (
        "dataset,end_date,entity,entry_date,geojson,geometry,json,name,organisation_entity,point,prefix,reference,start_date,typology",
        [
            {
                "dataset": "local-authority",
                "entity": "195",
                "entry_date": "2024-06-26",
                "json": json.dumps(
                    {
                        "addressbase-custodian": "4720",
                        "billing-authority": "E4704",
                        "combined-authority": "WYCA",
                        "local-authority-district": "E08000035",
                        "local-authority-type": "MD",
                        "local-planning-authority": "E60000071",
                        "local-resilience-forum": "west-yorkshire",
                        "opendatacommunities-uri": "http://opendatacommunities.org/id/metropolitan-district-council/leeds",
                        "parliament-thesaurus": "42600",
                        "region": "E12000003",
                        "statistical-geography": "E08000035",
                        "twitter": "LeedsCC_News",
                        "website": "https://www.leeds.gov.uk",
                        "wikidata": "Q6515870",
                        "wikipedia": "Leeds_City_Council",
                    }
                ),
                "name": "Leeds City Council",
                "organisation_entity": "195",
                "prefix": "local-authority",
                "reference": "LDS",
                "start_date": "1905-06-08",
                "typology": "test",
            }
        ],
    ),
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


@pytest.fixture(scope="session")
def dataset_dir(tmp_path_factory):
    dataset_dir = tmp_path_factory.mktemp("dataset")

    for filename, (fieldnames, rows) in dataset_data.items():
        with open(os.path.join(dataset_dir, filename), "w") as f:
            writer = csv.DictWriter(f, fieldnames.split(","))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    return dataset_dir


# Check a row contains the data we expect, and only the data we expect
def check_row(row, expected):
    for key, value in expected.items():
        assert key in row
        assert row[key] == value
    for key, value in row.items():
        if key not in expected:
            assert row[key] == ""


expected_row = {
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
}


def test_organisation_package_from_flattened(
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
        check_row(rows[0], expected_row)


def test_organisation_package_from_dataset(
    tmp_path,
    specification_dir,
    dataset_dir,
):
    package = OrganisationPackage(
        specification_dir=specification_dir,
        dataset_dir=dataset_dir,
        path=os.path.join(tmp_path, "organisation.csv"),
    )

    # create package
    package.create()

    # Check the output
    with open(package.path, "r", encoding="UTF8", newline="") as f:
        rows = [row for row in csv.DictReader(f)]

        assert len(rows) == 1
        check_row(rows[0], expected_row)
