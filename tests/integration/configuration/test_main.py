import pytest
import pandas as pd

from digital_land.specification import Specification
from digital_land.configuration.main import Config


@pytest.fixture(scope="module")
def test_spec():
    """
    A test specification for this module, may need to be updated if the spec
    changes
    """
    field = {
        "dataset": {"datatype": "string", "field": "dataset", "cardinality": 1},
        "end-date": {"datatype": "datetime", "field": "end-date", "cardinality": 1},
        "entry-date": {"datatype": "datetime", "field": "entry-date", "cardinality": 1},
        "entity-maximum": {
            "datatype": "integer",
            "field": "entity-maximum",
            "cardinality": 1,
        },
        "entity-minimum": {
            "datatype": "integer",
            "field": "entity-minimum",
            "cardinality": 1,
        },
        "organisation": {
            "datatype": "curie",
            "field": "organisation",
            "cardinality": 1,
        },
        "start-date": {"datatype": "datetime", "field": "start-date", "cardinality": 1},
    }

    schema = {
        "entity-organisation": {
            "schema": "entity-organisation",
            "fields": [
                "dataset",
                "end-date",
                "entry-date",
                "entity-maximum",
                "entity-minimum",
                "organisation",
                "start-date",
            ],
        }
    }
    spec = Specification()
    spec.field = field
    spec.schema = schema
    return spec


class TestConfig:
    def test_create(self, test_spec, tmp_path):
        # assert False, test_spec.field
        sqlite_path = tmp_path / "config.sqlite"
        tables = {"entity-organisation": "pipeline"}
        config = Config(path=sqlite_path, specification=test_spec, tables=tables)
        config.create()

        # get list of tables
        config.connect()
        config.create_cursor()
        config.execute("SELECT name FROM sqlite_master WHERE type='table';")

        # Fetch all results and store them in a list
        tables = [row[0] for row in config.cursor.fetchall()]

        config.disconnect()

        assert sqlite_path.exists()
        assert "entity_organisation" in tables

    def test_load(self, test_spec, tmp_path):

        # create example csv in temp_path
        test_data = [
            {
                "dataset": "conservation-area",
                "entity-minimum": 44000001,
                "entity-maximum": 44000001,
                "organisation": "local-authority:SAL",
            }
        ]
        test_df = pd.DataFrame(test_data)
        test_df.to_csv(tmp_path / "entity-organisation.csv")

        # assert False, test_spec.field
        sqlite_path = tmp_path / "config.sqlite"
        tables = {"entity-organisation": "pipeline"}
        config = Config(path=sqlite_path, specification=test_spec, tables=tables)
        config.create()

        tables = {"entity-organisation": str(tmp_path)}
        config.load(tables)

        # get tables
        config.connect()
        config.create_cursor()
        config.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in config.cursor.fetchall()]

        config.execute("SELECT COUNT(*) FROM entity_organisation;")

        # Fetch the result (the row count)
        row_count = config.cursor.fetchone()[0]

        assert "entity_organisation" in tables
        assert row_count == 1
