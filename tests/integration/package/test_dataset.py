import pytest
import csv
import os
import urllib.request
import pandas as pd
import sqlite3

from digital_land.package.dataset import DatasetPackage
from digital_land.package.package import Specification
from digital_land.organisation import Organisation


@pytest.fixture
def transformed_fact_resources():
    input_data = [
        {
            "entity": "44006677",
            "entry-date": "2021-09-06",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
        },
        {
            "entity": "44006677",
            "entry-date": "2022-11-02",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
        },
    ]

    return input_data


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


@pytest.fixture(scope="session")
def organisation_csv(tmp_path_factory):
    organisation_dir = tmp_path_factory.mktemp("organisation")

    organisation_path = organisation_dir / "organisation.csv"
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )
    return organisation_path


@pytest.fixture(scope="session")
def transformed_file_path(tmp_path_factory):
    transformed_dir = tmp_path_factory.mktemp("transformed")
    resource_hash = "435138a11d3c50f7ff217c8e938b44376ef5938543702b0bd7c06cd3a4e7422c"
    out_file_path = transformed_dir / f"{resource_hash}.csv"

    # create transformed file source
    row = {
        "end-date": "",
        "entity": "700000",
        "entry-date": "2023-10-03",
        "entry-number": "1",
        "fact": "7fa1bd3c802494f4998599b80c828146f621987da954d3490a4b98a5f24a8b4d",
        "field": "entry-date",
        "reference-entity": "",
        "resource": "47e8c774370b10c803da048f4f1d98cfb028d25e01408314c92f0ac74cbee12b",
        "start-date": "",
        "value": "2023-10-03",
    }
    fieldnames = row.keys()

    with open(out_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    return str(out_file_path)


@pytest.fixture
def blank_patch_csv(tmp_path):
    patch_path = os.path.join(tmp_path, "organisation.csv")
    fieldnames = ["dataset", "resource", "field", "pattern", "value"]
    with open(patch_path, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    return patch_path


def test_load_old_entities_entities_outside_of_range_are_removed(tmp_path):
    # create custom specification to feed in
    schema = {
        "old-entity": {
            "fields": [
                "end-date",
                "entity",
                "entry-date",
                "notes",
                "old-entity",
                "start-date",
                "status",
            ]
        },
        "conservation-area": {"entity-minimum": "1", "entity-maximum": "2"},
        "entity": {"fields": []},
    }
    field = {
        "end-date": {
            "datatype": "datetime",
        },
        "entity": {
            "datatype": "integer",
        },
        "entry-date": {"datatype": "datetime"},
        "notes": {"datatype": "text"},
        "old-entity": {
            "datatype": "integer",
        },
        "start-date": {
            "datatype": "datetime",
        },
        "status": {
            "datatype": "string",
        },
    }

    specification = Specification(schema=schema, field=field)
    organisation = Organisation(organisation={})

    # write data to csv as we only seem to load from csv
    data = [
        {
            "end-date": "",
            "entity": "",
            "entry-date": "",
            "notes": "",
            "old-entity": "1",
            "start-date": "",
            "status": "410",
        },
        {
            "end-date": "",
            "entity": "",
            "entry-date": "",
            "notes": "",
            "old-entity": "3",
            "start-date": "",
            "status": "410",
        },
    ]

    old_entity_path = os.path.join(tmp_path, "old-entity.csv")
    with open(old_entity_path, "w") as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        w.writerows(data)

    # create sqlite db
    sqlite3_path = os.path.join(tmp_path, "test.sqlite3")

    # create class on sqlite db with old_entity table in it
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification=specification,
    )
    package.connect()
    package.create_cursor()
    package.create_table("old-entity", schema["old-entity"]["fields"], "old-entity")

    # run load old_entity function from csv above
    package.load_old_entities(old_entity_path)
    package.disconnect()
    # test entity out of range is not in sqlite
    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT * FROM old_entity;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    assert results["old_entity"].max() <= 2
    assert results["old_entity"].min() >= 1


def test_entry_date_upsert_uploads_newest_date(
    specification_dir,
    organisation_csv,
    transformed_fact_resources,
    tmp_path,
):
    dataset = "conservation-area"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(organisation_csv)
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    # create package
    package.create()

    # run upload to fact table not fact resource for testing the upsert
    package.connect()
    package.create_cursor()
    fact_fields = package.specification.schema["fact"]["fields"]
    fact_conflict_fields = ["fact"]
    fact_update_fields = [
        field for field in fact_fields if field not in fact_conflict_fields
    ]
    for row in transformed_fact_resources:
        package.entry_date_upsert(
            "fact", fact_fields, row, fact_conflict_fields, fact_update_fields
        )
    package.commit()
    package.disconnect()

    # retrieve results
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")
    cols = [column[0] for column in package.cursor.description]
    actual_result = pd.DataFrame.from_records(
        package.cursor.fetchall(), columns=cols
    ).to_dict(orient="records")
    expected_result = [
        {
            "end_date": "",
            "entity": 44006677,
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "entry_date": "2022-11-02",
            "reference_entity": "",
            "start_date": "",
            "value": "Burghwallis",
        }
    ]

    assert actual_result == expected_result, "actual result does not match query"


def test_load_issues_uploads_issues_from_csv(tmp_path):
    # create custom specification to feed in
    schema = {
        "issue": {
            "fields": [
                "end-date",
                "entry-date",
                "entry-number",
                "field",
                "issue-type",
                "line-number",
                "dataset",
                "resource",
                "start-date",
                "value",
            ]
        },
        "conservation-area": {"entity-minimum": "1", "entity-maximum": "2"},
        "entity": {"fields": []},
    }
    field = {
        "end-date": {
            "datatype": "datetime",
        },
        "entry-date": {"datatype": "datetime"},
        "entry-number": {"datatype": "integer"},
        "field": {"datatype": "string"},
        "issue-type": {
            "datatype": "string",
        },
        "line-number": {
            "datatype": "datetime",
        },
        "dataset": {
            "datatype": "string",
        },
        "resource": {
            "datatype": "string",
        },
        "start-date": {
            "datatype": "datetime",
        },
        "value": {
            "datatype": "text",
        },
    }

    specification = Specification(schema=schema, field=field)
    organisation = Organisation(organisation={})

    # write data to csv as we only seem to load from csv
    data = [
        {
            "end-date": "",
            "entry-date": "",
            "entry-number": "1",
            "field": "test",
            "issue-type": "test",
            "line-number": "2",
            "dataset": "conservation-area",
            "resource": "efdec",
            "start-date": "",
            "value": "test",
        },
    ]

    issue_path = os.path.join(tmp_path, "efdec.csv")
    with open(issue_path, "w") as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        w.writerows(data)

    # create sqlite db
    sqlite3_path = os.path.join(tmp_path, "test.sqlite3")

    # create class on sqlite db with old_entity table in it
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification=specification,
    )
    package.connect()
    package.create_cursor()
    package.create_table("issue", schema["issue"]["fields"], "issue")

    # run load old_entity function from csv above
    package.load_issues(issue_path)
    package.disconnect()
    # test entity out of range is not in sqlite
    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT * FROM issue;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    assert len(results) > 0


def test_load_transformed_creates_expected_entries(
    specification_dir,
    organisation_csv,
    transformed_file_path,
    tmp_path,
):
    dataset = "listed-building"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(organisation_csv)
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,
    )

    # create package
    package.create()
    package.connect(optimised=True)

    package.load_transformed(transformed_file_path)

    package.disconnect()

    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT
            (select count(*) from fact) as f_count,
            (select count(*) from fact_resource) as fr_count,
            (select count(*) from column_field) as cf_count,
            (select count(*) from dataset_resource) as dr_count,
            (select count(*) from entity) as e_count,
            (select count(*) from issue) as i_count,
            (select count(*) from old_entity) as oe_count;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        qry_result = cursor.fetchall()
        results_df = pd.DataFrame.from_records(data=qry_result, columns=cols)

        assert results_df["f_count"].values[0] == 1
        assert results_df["fr_count"].values[0] == 1
        assert results_df["cf_count"].values[0] == 1
        assert results_df["dr_count"].values[0] == 1
        assert results_df["e_count"].values[0] == 0
        assert results_df["i_count"].values[0] == 0
        assert results_df["oe_count"].values[0] == 0


def test_load_entities_creates_expected_entries(
    specification_dir,
    organisation_csv,
    transformed_file_path,
    tmp_path,
):
    dataset = "listed-building"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(organisation_csv)
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,
    )

    # create package
    package.create()
    package.connect(optimised=True)

    package.load_transformed(transformed_file_path)
    package.load_entities()

    package.disconnect()

    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT
            (select count(*) from fact) as f_count,
            (select count(*) from fact_resource) as fr_count,
            (select count(*) from column_field) as cf_count,
            (select count(*) from dataset_resource) as dr_count,
            (select count(*) from entity) as e_count,
            (select count(*) from issue) as i_count,
            (select count(*) from old_entity) as oe_count;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        qry_result = cursor.fetchall()
        results_df = pd.DataFrame.from_records(data=qry_result, columns=cols)

        assert results_df["f_count"].values[0] == 1
        assert results_df["fr_count"].values[0] == 1
        assert results_df["cf_count"].values[0] == 1
        assert results_df["dr_count"].values[0] == 1
        assert results_df["e_count"].values[0] == 1
        assert results_df["i_count"].values[0] == 0
        assert results_df["oe_count"].values[0] == 0


def test_insert_many_creates_expected_entries(
    specification_dir,
    organisation_csv,
    transformed_file_path,
    tmp_path,
):
    dataset = "listed-building"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(organisation_csv)
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,
    )

    package.create()
    package.connect(optimised=True)

    fact_resource_fields = [
        "end-date",
        "fact",
        "entry-date",
        "entry-number",
        "resource",
        "start-date",
    ]

    insert_rows = [
        (
            "",
            "7fa1bd3c802494f4998599b80c828146f621987da954d3490a4b98a5f24a8b4d",
            "2023-10-03",
            "1",
            "47e8c774370b10c803da048f4f1d98cfb028d25e01408314c92f0ac74cbee12b",
            "",
        ),
        (
            "",
            "c7bc75878771bb9d39c2ed73d2797a8f910c00dd1813542d19cf6d40bd807bb2",
            "2023-10-03",
            "1",
            "47e8c774370b10c803da048f4f1d98cfb028d25e01408314c92f0ac74cbee12b",
            "",
        ),
    ]

    package.create_cursor()
    package.insert_many("fact-resource", fact_resource_fields, insert_rows, upsert=True)
    package.commit()

    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT
            (select count(*) from fact_resource) as fr_count;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        qry_result = cursor.fetchall()
        results_df = pd.DataFrame.from_records(data=qry_result, columns=cols)

        assert results_df["fr_count"].values[0] == 2
