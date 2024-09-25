import pytest

import pandas as pd

from pathlib import Path

from digital_land.specification import Specification


# mock the init function of specification to stop the loading of things
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


# set of fixtures to represent the specification for this module
@pytest.fixture(scope="module")
def spec_dir(tmp_path_factory):
    # Create a temporary directory
    return tmp_path_factory.mktemp("specification")


@pytest.fixture(scope="module")
def test_schema_csv(spec_dir):
    """
    create a  test schema for testing the specification
    """
    schema_path = spec_dir / "schema.csv"
    data = pd.DataFrame([{"schema": "battlefield", "key-field": "entity"}])
    data.to_csv(schema_path)

    return schema_path


@pytest.fixture(scope="module")
def test_schema_field_csv(spec_dir):
    """
    create a  test schema for testing the specification
    """
    schema_path = spec_dir / "schema-field.csv"
    data = pd.DataFrame([{"schema": "battlefield", "field": "entity"}])
    data.to_csv(schema_path)

    return schema_path


class TestSpecification:
    def test_load_schema_loads_successfully(
        self, spec_dir: Path, test_schema_csv: Path, mocker
    ):
        # create spec without folder to just test loading on schema check no schema loaded
        mocker.patch("digital_land.specification.Specification.__init__", mock_init)
        spec = Specification()
        assert (
            len(spec.schema) == 0
        ), f"schema  has  been loaded when it shouldnt have: {spec.schema}"

        # load schema
        spec.load_schema(spec_dir)

        assert len(spec.schema) > 0, "not schemas have been loaded"

    def test_load_schema_field_loads_successfully(
        self, spec_dir: Path, test_schema_csv: Path, test_schema_field_csv: Path, mocker
    ):
        # create spec without folder to just test loading on schema check no schema loaded
        mocker.patch("digital_land.specification.Specification.__init__", mock_init)
        spec = Specification()

        # load schema
        spec.load_schema(spec_dir)
        spec.load_schema_field(spec_dir)

        assert len(spec.schema_field) > 0, "not schemas have been loaded"
        assert (
            len(spec.schema["battlefield"]["fields"]) > 0
        ), "no fields added to schema"
