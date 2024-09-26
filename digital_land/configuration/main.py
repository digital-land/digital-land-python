"""
Module to contain the primary Classes and functions for the configuration package
The config class is primarily how to access all the different configuration classes.

"""

from pathlib import Path

from digital_land.package.sqlite import SqlitePackage
from digital_land.package.package import Specification


class Config(SqlitePackage):
    """
    A class which represents configuration to be used to control collection and pipeline processes.
    """

    def __init__(
        self,
        path: Path,
        specification: Specification,
        tables: dict = None,
        indexes=None,
    ):
        self.path = Path(path)
        self.specification = specification
        self.tables = tables or {
            "entity-organisation": "pipeline",
        }

        self.indexes = {
            "entity-organisation": ["organisation", "entity-minimum", "entity-maximum"],
        }

        self._spatialite = None

    def create(self):
        self.create_database()
        self.create_indexes()
        self.disconnect()

    # TODO This provides access to the entity organisations. Should the config have an object for each File
    def get_entity_organisation(self, entity):
        self.connect()
        self.create_cursor()
        self.cursor.execute(
            f"select organisation from entity_organisation where entity_minimum <= {entity} and entity_maximum >= {entity}"
        )
        row = self.cursor.fetchone()

        result = row[0] if row else None
        self.disconnect()

        return result

    def load(self, tables=None):
        tables = tables or self.tables
        self.connect()
        self.create_cursor()
        self.drop_indexes()
        super().load(tables)
        self.create_indexes()
        self.disconnect()
