import pytest
import pandas as pd
import logging
import json

from digital_land.specification import Specification
from digital_land.configuration.main import Config


def mock_init(self):
    self.dataset = {}
    self.dataset_names = []
    self.schema = {}
    self.schema_names = []
    self.dataset_schema = {}
    self.field = {}
    self.field_names = []
    self.datatype = {}
    self.datatype_names = []
    self.schema_field = {}
    self.typology = {}
    self.pipeline = {}


@pytest.fixture()
def test_spec(mocker):
    """
    A test specification for this module, may need to be updated if the spec
    changes
    """
    field = {
        "dataset": {"datatype": "string", "field": "dataset", "cardinality": 1},
        "datasets": {"datatype": "string", "field": "datasets", "cardinality": "n"},
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
        "operation": {
            "datatype": "string",
            "field": "operation",
            "cardinality": 1,
        },
        "parameters": {
            "datatype": "string",
            "field": "parameters",
            "cardinality": "n",
        },
        "name": {
            "datatype": "string",
            "field": "name",
            "cardinality": 1,
        },
        "description": {
            "datatype": "string",
            "field": "description",
            "cardinality": 1,
        },
        "notes": {
            "datatype": "string",
            "field": "notes",
            "cardinality": 1,
        },
        "severity": {
            "datatype": "string",
            "field": "severity",
            "cardinality": 1,
        },
        "responsibility": {
            "datatype": "string",
            "field": "responsibility",
            "cardinality": 1,
        },
        "organisations": {
            "datatype": "string",
            "field": "datasets",
            "cardinality": "n",
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
        },
        "expect": {
            "schema": "expect",
            "fields": [
                "datasets",
                "organisations",
                "operation",
                "parameters",
                "name",
                "description",
                "notes",
                "severity",
                "responsibility",
                "end-date",
                "entry-date",
                "start-date",
            ],
        },
    }
    mocker.patch("digital_land.specification.Specification.__init__", mock_init)
    spec = Specification()
    spec.field = field
    spec.schema = schema
    return spec


class TestConfig:
    def test_create(self, test_spec, tmp_path):
        # assert False, test_spec.field
        sqlite_path = tmp_path / "config.sqlite"
        tables = {"entity-organisation": "pipeline", "expect": "pipeline"}
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

    def test_get_entity_organisation_returns_organisation(self, test_spec, tmp_path):
        test_data = [
            {
                "dataset": "conservation-area",
                "entity-minimum": 44000001,
                "entity-maximum": 44000001,
                "organisation": "local-authority:SAL",
            }
        ]
        sqlite_path = tmp_path / "config.sqlite"
        tables = {"entity-organisation": str(tmp_path)}
        config = Config(path=sqlite_path, specification=test_spec, tables=tables)
        test_df = pd.DataFrame(test_data)
        test_df.to_csv(tmp_path / "entity-organisation.csv")
        config.create()

        tables = {"entity-organisation": str(tmp_path)}
        config.load(tables)

        org = config.get_entity_organisation(44000001)
        assert org == "local-authority:SAL"

    def test_get_expectation_rules_returns_rules(self, test_spec, tmp_path):
        test_data = [
            {
                "datasets": "conservation-area;article-4-direction-area",
                "organisations": "local-authority:DNC",
                "operation": "test",
                "parameters": '{"test":"test"}',
                "name": "local-authority:SAL",
            }
        ]
        sqlite_path = tmp_path / "config.sqlite"
        tables = {"expect": str(tmp_path)}
        config = Config(path=sqlite_path, specification=test_spec, tables=tables)
        test_df = pd.DataFrame(test_data)
        test_df.to_csv(tmp_path / "expect.csv")
        config.create()
        config.load()

        rules = config.get_expectation_rules("conservation-area")
        logging.debug(f"The expectation rules produced:\n{json.dumps(rules, indent=4)}")
        for key in test_data[0]:
            assert test_data[0][key] == rules[0][key], rules

        assert type(rules[0]["parameters"]) is str
