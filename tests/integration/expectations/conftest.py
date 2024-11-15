import pytest
import os
import spatialite


@pytest.fixture
def sqlite3_with_entity_tables_path(tmp_path):
    dataset_path = os.path.join(tmp_path, "test.sqlite3")

    create_entity_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """

    create_old_entity_table_sql = """
        CREATE TABLE old_entity (
            old_entity INTEGER PRIMARY KEY,
            entity INTEGER
        );
    """

    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.execute(create_old_entity_table_sql)

    return dataset_path
