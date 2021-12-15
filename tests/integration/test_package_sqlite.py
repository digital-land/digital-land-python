import os
import shutil

import pytest
import sqlite3

from digital_land.package.sqlite import SqlitePackage


@pytest.fixture
def setup_cwd_for_pipeline_ingest(tmpdir):
    # Relative paths used within function
    cwd = os.getcwd()
    os.chdir(tmpdir)

    yield

    os.chdir(cwd)


def test_sqlite_package(mocker, tmpdir, setup_cwd_for_pipeline_ingest):
    """
    Simple test to check that we can create & populate a trivial database

    Database should contain data from digital_land/tests/data/test_entity_dataset/
    """
    # Setup
    database_path = tmpdir / 'tempdataset.sqlite3'
    expected_entity_result = [('', '', 110042408, '2021-12-01', '', 'WILK WOOD', '', '', '', '', '')]
    expected_metadata_last_modified_result = '2021-12-14T14:57:07.714295'
    mocker.patch.object(SqlitePackage, '_get_current_datetime', return_value=expected_metadata_last_modified_result)
    tables = {
        "entity": "dataset",
    }

    indexes = {
        "entity": ["entity", "typology", "dataset", "reference", "organisation-entity", "json"],
    }
    shutil.copytree(
        os.path.join(os.path.dirname(__file__), '../data/test_entity_dataset'),
        tmpdir, dirs_exist_ok=True
    )
    # Call
    package = SqlitePackage("entity", tables=tables, indexes=indexes)
    package.spatialite()
    package.create(database_path)
    # Assert
    conn = sqlite3.connect(database_path)
    assert list(conn.execute('select * from entity')) == expected_entity_result
    assert list(conn.execute('select * from metadata')) == [(1, expected_metadata_last_modified_result),]
